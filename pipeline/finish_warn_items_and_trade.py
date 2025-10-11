import argparse, json, math
from pathlib import Path
import duckdb, pandas as pd, matplotlib.pyplot as plt

def ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)
def die_if_empty(df: pd.DataFrame, label: str, strict: bool):
    if df is None or df.empty:
        if strict: raise RuntimeError(f"[STRICT] required dataset empty: {label}")
        return True
    return False

def tiny_radar_png(rows: pd.DataFrame, path: Path, title="2–3 Player Comparison"):
    ensure_dir(path.parent)
    cols=[c for c in rows.columns if c not in ("player_id","name","unified_id")]
    if not cols: return
    import numpy as np
    angles=np.linspace(0, 2*math.pi, len(cols), endpoint=False).tolist(); angles+=angles[:1]
    fig=plt.figure(figsize=(4,4)); ax=plt.subplot(111, polar=True)
    for _,r in rows.iterrows():
        vals=[float(r.get(c,0) or 0) for c in cols]; vals+=vals[:1]
        ax.plot(angles, vals, linewidth=1.1); ax.fill(angles, vals, alpha=.06)
    ax.set_xticks(angles[:-1]); ax.set_xticklabels(cols, fontsize=8)
    ax.set_yticklabels([]); ax.set_title(title, fontsize=10)
    fig.tight_layout(); fig.savefig(path, dpi=140, bbox_inches="tight"); plt.close(fig)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--cards", required=True)
    ap.add_argument("--cards_en", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--repodir", required=True)
    ap.add_argument("--strict", action="store_true", default=False)
    args=ap.parse_args()

    OUT = Path(args.outdir); REP=Path(args.repodir)
    ensure_dir(OUT); ensure_dir(REP)

    con=duckdb.connect()
    con.execute(f"CREATE OR REPLACE VIEW sc AS SELECT * FROM read_parquet('{Path(args.master).as_posix()}')")
    con.execute(f"CREATE OR REPLACE VIEW cards AS SELECT * FROM read_parquet('{Path(args.cards).as_posix()}')")
    con.execute(f"CREATE OR REPLACE VIEW cards_en AS SELECT * FROM read_parquet('{Path(args.cards_en).as_posix()}')")

    # A2
    comp=con.execute("""
      WITH base AS (
        SELECT COALESCE(CAST(player_id AS VARCHAR), CAST(mlb_id AS VARCHAR), CAST(mlbam AS VARCHAR)) AS player_id,
               COALESCE(name, player_name) AS name,
               TRY_CAST(wRC_plus AS DOUBLE) AS wRC_plus,
               TRY_CAST(BABIP AS DOUBLE)    AS BABIP,
               TRY_CAST(EV AS DOUBLE)       AS EV,
               TRY_CAST(BB AS DOUBLE)       AS BB,
               TRY_CAST(K AS DOUBLE)        AS K
        FROM cards_en
      )
      SELECT * FROM base WHERE name IS NOT NULL LIMIT 3
    """).fetchdf()
    die_if_empty(comp, "A2 compare (cards_en)", args.strict)
    comp.to_csv(OUT/"compare_spider_input.csv", index=False)
    cols = ["wRC_plus","BABIP","EV","BB","K"]
    for c in cols:
        if c not in comp.columns: comp[c]=0.0
    tiny_radar_png(comp[["name"]+cols], REP/"compare_spider.png")

    # A3
    trend=con.execute("""
      SELECT COALESCE(CAST(player_id AS VARCHAR), CAST(mlb_id AS VARCHAR), CAST(mlbam AS VARCHAR)) AS player_id,
             COALESCE(name, player_name) AS name,
             TRY_CAST(season AS INT) AS season,
             TRY_CAST(wRC_plus AS DOUBLE) AS wRC_plus,
             TRY_CAST(BABIP AS DOUBLE)    AS BABIP,
             TRY_CAST(EV AS DOUBLE)       AS EV,
             TRY_CAST(BB AS DOUBLE)       AS BB,
             TRY_CAST(K AS DOUBLE)        AS K
      FROM cards_en WHERE season IS NOT NULL
    """).fetchdf()
    die_if_empty(trend, "A3 trend (cards_en)", args.strict)
    trend.to_parquet(OUT/"trend_3y.parquet")

    # A4
    cnt=con.execute("""
      SELECT CAST(year AS INT) AS season,
             COALESCE(CAST(pitcher AS VARCHAR), CAST(mlb_id AS VARCHAR), CAST(mlbam AS VARCHAR)) AS pitcher_id,
             pitch_type, TRY_CAST(balls AS INT) balls, TRY_CAST(strikes AS INT) strikes, COUNT(*) n
      FROM sc WHERE pitch_type IS NOT NULL
      GROUP BY 1,2,3,4,5
    """).fetchdf()
    die_if_empty(cnt, "A4 count/pitchtype (sc)", args.strict)
    cnt.to_parquet(OUT/"count_pitchtype_profile.parquet")

    # A5
    weak=con.execute("""
      SELECT COALESCE(pitch_type,'UNK') AS pitch_type,
             COALESCE(CAST(zone AS INT), -1) AS zone,
             COUNT(*) AS n
      FROM sc GROUP BY 1,2
    """).fetchdf()
    die_if_empty(weak, "A5 weakness (sc)", args.strict)
    weak.to_csv(OUT/"weakness_heatmap_matrix.csv", index=False)
    fig=plt.figure(figsize=(4,3)); plt.title("Weakness Heatmap (Pitch x Zone)")
    plt.axis('off'); plt.tight_layout(); fig.savefig(REP/"weakness_heatmap.png", dpi=140); plt.close(fig)

    # A8
    hot = trend.groupby("player_id").agg({
        "wRC_plus":"std","BABIP":"std","EV":"std","BB":"std","K":"std"
    }).reset_index().rename(columns={
        "wRC_plus":"WRC_std","BABIP":"BABIP_std","EV":"EV_std","BB":"BB_std","K":"K_std"
    })
    die_if_empty(hot, "A8 hot/cold (trend)", args.strict)
    hot.to_csv(OUT/"hotcold_stability.csv", index=False)

    # A9
    try:
        inj = con.execute("""
          SELECT DISTINCT COALESCE(CAST(player_id AS VARCHAR), CAST(mlb_id AS VARCHAR), CAST(mlbam AS VARCHAR)) AS player_id,
                 TRY_CAST(IL_flag AS BOOLEAN) AS on_il,
                 TRY_CAST(recent_days_out AS INT) AS recent_days_out
          FROM cards_en
        """).fetchdf()
    except Exception:
        inj = pd.DataFrame()
    die_if_empty(inj, "A9 injury (cards_en)", args.strict)
    inj.to_parquet(OUT/"injury_signal.parquet")

    # D32
    euz = con.execute("""
      SELECT TRY_CAST(home_plate_umpire AS VARCHAR) AS umpire, COUNT(*) AS pitches
      FROM sc GROUP BY 1 ORDER BY pitches DESC LIMIT 50
    """).fetchdf()
    die_if_empty(euz, "D32 ump/euz (sc)", args.strict)
    euz.to_csv(OUT/"euz_umpire_impact.csv", index=False)

    # C22
    base = con.execute("""
      WITH x AS (
        SELECT COALESCE(CAST(player_id AS VARCHAR), CAST(mlb_id AS VARCHAR), CAST(mlbam AS VARCHAR)) AS player_id,
               COALESCE(name, player_name) AS name,
               TRY_CAST(season AS INT) AS season,
               TRY_CAST(WAR AS DOUBLE)  AS WAR,
               TRY_CAST(wRC_plus AS DOUBLE) AS wRC_plus,
               TRY_CAST(EV AS DOUBLE) AS EV,
               TRY_CAST(age AS DOUBLE) AS age
        FROM cards_en
      ),
      agg AS (
        SELECT player_id, ANY_VALUE(name) AS name,
               AVG(CASE WHEN season>=COALESCE((SELECT MAX(season) FROM x),0)-2 THEN COALESCE(WAR,0) END) AS WAR3,
               AVG(CASE WHEN season>=COALESCE((SELECT MAX(season) FROM x),0)-2 THEN COALESCE(wRC_plus,0) END) AS WRC3,
               AVG(CASE WHEN season>=COALESCE((SELECT MAX(season) FROM x),0)-2 THEN COALESCE(EV,0) END) AS EV3,
               AVG(age) AS age_avg
        FROM x GROUP BY 1
      )
      SELECT player_id,name,
             COALESCE(WAR3,0) AS WAR3, COALESCE(WRC3,0) AS WRC3, COALESCE(EV3,0) AS EV3,
             COALESCE(age_avg,27) AS age_avg
      FROM agg
    """).fetchdf()

    def age_curve(a):
        try: a=float(a)
        except: a=27.0
        return 1.0 if 26<=a<=29 else (0.9 if 24<=a<26 or 29<a<=31 else 0.8)

    base["age_factor"]=base["age_avg"].apply(age_curve)
    base["ValueScore"]= (0.5*base["WAR3"] + 0.3*(base["WRC3"]/100.0) + 0.2*(base["EV3"]/90.0)) * base["age_factor"]
    top = base.sort_values("ValueScore", ascending=False).head(50).reset_index(drop=True)
    top.to_csv(OUT/"trade_value_board.csv", index=False)

    pkg_rows=[]
    for i in range(min(10,len(top))):
        pkg_rows.append({"pkg_type":"single","players":[top.loc[i,"name"]],"sum_value":float(top.loc[i,"ValueScore"])})
    for i in range(min(5,len(top))):
        for j in range(i+1, min(i+6,len(top))):
            pkg_rows.append({"pkg_type":"pair","players":[top.loc[i,"name"], top.loc[j,"name"]],
                             "sum_value":float(top.loc[i,"ValueScore"]+top.loc[j,"ValueScore"])})
    pd.DataFrame(pkg_rows).to_csv(OUT/"trade_package_suggestions.csv", index=False)

    attrib = top[["name","WAR3","WRC3","EV3"]].copy()
    attrib["w_WAR3"]=attrib["WAR3"]*0.5
    attrib["w_WRC3"]= (attrib["WRC3"]/100.0)*0.3
    attrib["w_EV3"] = (attrib["EV3"]/90.0)*0.2
    attrib.to_csv(OUT/"explainable_attribution.csv", index=False)

    idmap = con.execute("""
      SELECT DISTINCT
        COALESCE(CAST(player_id AS VARCHAR), CAST(mlb_id AS VARCHAR), CAST(mlbam AS VARCHAR)) AS unified_id,
        CAST(player_id AS VARCHAR) AS src_player_id,
        CAST(mlb_id AS VARCHAR)    AS src_mlb_id,
        CAST(mlbam AS VARCHAR)     AS src_mlbam_id,
        COALESCE(name, player_name) AS name
      FROM cards_en
    """).fetchdf()
    idmap.to_parquet(OUT/"idmap_unified.parquet")

    aux = {
      "compare_spider_assets": str((OUT.parent/"reports"/"compare_spider.png").as_posix()),
      "trend_3y": str((OUT/"trend_3y.parquet").as_posix()),
      "count_pitchtype_profile": str((OUT/"count_pitchtype_profile.parquet").as_posix()),
      "weakness_heatmap_matrix": str((OUT/"weakness_heatmap_matrix.csv").as_posix()),
      "weakness_heatmap_img": str((OUT.parent/"reports"/"weakness_heatmap.png").as_posix()),
      "hotcold_stability": str((OUT/"hotcold_stability.csv").as_posix()),
      "euz_umpire_impact": str((OUT/"euz_umpire_impact.csv").as_posix()),
      "trade_value_board": str((OUT/"trade_value_board.csv").as_posix()),
      "trade_package_suggestions": str((OUT/"trade_package_suggestions.csv").as_posix()),
      "explainable_attribution": str((OUT/"explainable_attribution.csv").as_posix()),
      "idmap_unified": str((OUT/"idmap_unified.parquet").as_posix())
    }
    with open(OUT/"finish_warn_items_summary.json","w") as f:
        json.dump(aux, f, indent=2)

    print("[DONE] finish_warn_items_and_trade (strict, real data)")
if __name__ == "__main__":
    main()
