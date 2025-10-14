#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 1) streamlit_app.py (사용자가 제공한 내용 그대로)
cat > "${ROOT}/streamlit_app.py" <<'PY'
# streamlit_app.py — Portfolio + Real Data (reports kept, player+trade added)
import streamlit as st
import duckdb, pandas as pd, numpy as np
from pathlib import Path
from PIL import Image, UnidentifiedImageError

st.set_page_config(page_title="Co-GM Assistant — Streamlit", layout="wide")
st.title("⚾ Co-GM Assistant — Streamlit Portfolio Version")
st.caption("리포트/비주얼 유지 + 선수 실데이터 검색 + 트레이드 평가")

ROOT = Path(__file__).parent
OUT = ROOT / "output"
REP = OUT / "reports"
SUM = OUT / "summaries"

STATCAST = OUT / "statcast_ultra_full_clean.parquet"
CARDS    = OUT / "player_cards_all.parquet"

# ---------- helpers ----------
def list_images(folder: Path):
    if not folder.exists(): return []
    imgs = []
    for p in sorted(folder.iterdir()):
        if not p.is_file(): continue
        if p.suffix.lower() not in {".png",".jpg",".jpeg",".webp",".gif"}: continue
        try:
            # LFS pointer guard
            head = p.read_bytes()[:64]
            if head.startswith(b"version https://git-lfs.github.com"):
                continue
        except Exception:
            continue
        try:
            with Image.open(p) as im:
                im.verify()
            if p.stat().st_size > 200:
                imgs.append(p)
        except Exception:
            continue
    return imgs

def list_pdfs(folder: Path):
    if not folder.exists(): return []
    return [p for p in sorted(folder.glob("*.pdf")) if p.stat().st_size > 400]

NAME_CANDS = ["player_name","name","full_name","batter_name","pitcher_name","player","Player","Name"]
ID_CANDS   = ["player_id","mlbam_id","mlbamid","batter","pitcher","id","player_uid"]

def pick_col(cols, cand_list):
    for c in cand_list:
        if c in cols: return c
        # case-insensitive fallback
        for x in cols:
            if x.lower()==c.lower(): return x
    return None

def load_cards_sample(limit=300000):
    if not CARDS.exists(): return None
    con = duckdb.connect()
    try:
        # Load only a few useful columns if present
        rel = con.sql(f"""
            SELECT * FROM read_parquet('{CARDS.as_posix()}')
            LIMIT {limit}
        """).df()
        return rel
    finally:
        con.close()

def load_statcast_sample(limit=300000):
    if not STATCAST.exists(): return None
    con = duckdb.connect()
    try:
        rel = con.sql(f"""
            SELECT * FROM read_parquet('{STATCAST.as_posix()}')
            LIMIT {limit}
        """).df()
        return rel
    finally:
        con.close()

def build_value_table():
    # Try rich value first
    paths = [
        SUM/"role_fit_suggestions.csv",
        SUM/"player_value.csv",
        SUM/"leaderboard_entropy_top10.csv",  # fallback-ish
    ]
    for p in paths:
        if p.exists():
            try:
                df = pd.read_csv(p)
                return df
            except Exception:
                continue
    return None

# ---------- tabs ----------
tab_ov, tab_player, tab_visuals, tab_trade = st.tabs(
    ["Overview", "Player Search", "Visuals", "Trade Simulator"]
)

# ========== Overview ==========
with tab_ov:
    st.subheader("Data Overview")
    files = []
    if CARDS.exists():
        try:
            cards_cols = duckdb.sql(f"SELECT * FROM read_parquet('{CARDS.as_posix()}') LIMIT 0").df().columns.tolist()
            cnt = duckdb.sql(f"SELECT count(*) c FROM read_parquet('{CARDS.as_posix()}')").df()["c"][0]
            files.append(("player_cards_all.parquet", int(cnt), ", ".join(cards_cols[:8])))
        except Exception:
            files.append(("player_cards_all.parquet", "?", "(columns read failed)"))
    if STATCAST.exists():
        try:
            sc_cols = duckdb.sql(f"SELECT * FROM read_parquet('{STATCAST.as_posix()}') LIMIT 0").df().columns.tolist()
            cnt = duckdb.sql(f"SELECT count(*) c FROM read_parquet('{STATCAST.as_posix()}')").df()["c"][0]
            files.append(("statcast_ultra_full_clean.parquet", int(cnt), ", ".join(sc_cols[:8])))
        except Exception as e:
            st.warning(f"Failed to read statcast: {e}")
            files.append(("statcast_ultra_full_clean.parquet", 0, "(read error)"))
    st.dataframe(pd.DataFrame(files, columns=["file","rows","columns"]), use_container_width=True)

# ========== Player Search ==========
with tab_player:
    st.subheader("Player Search (partial OK)")
    q = st.text_input("선수 이름 또는 ID (부분 문자열 가능)", "")
    if "player_df" not in st.session_state:
        # load source preference: cards -> statcast
        src = load_cards_sample(limit=800000)
        if src is None:
            src = load_statcast_sample(limit=800000)
        st.session_state["player_df"] = src

    df = st.session_state.get("player_df")
    if df is None:
        st.error("사용 가능한 플레이어 소스가 없습니다. `output/player_cards_all.parquet` 또는 `output/statcast_ultra_full_clean.parquet`가 필요합니다.")
    else:
        cols = list(df.columns)
        name_col = pick_col(cols, NAME_CANDS)
        id_col   = pick_col(cols, ID_CANDS)

        with st.expander("Detected columns / mapping", expanded=False):
            st.write({"name_col": name_col, "id_col": id_col, "total_rows": len(df)})

        if not name_col and not id_col:
            st.warning("이 데이터셋에서 이름/ID 컬럼을 찾지 못했습니다. 최소 하나는 포함되어야 합니다.")
        else:
            if q.strip():
                mask = pd.Series([False]*len(df))
                if name_col:
                    mask = mask | df[name_col].astype(str).str.contains(q, case=False, na=False)
                if id_col and q.strip().isdigit():
                    mask = mask | (df[id_col].astype(str)==q.strip())
                hits = df.loc[mask].copy()
                # show a compact subset
                show_cols = []
                for c in [name_col, id_col, "season","team","teamName","league","pos","position"]:
                    if c and c in cols and c not in show_cols:
                        show_cols.append(c)
                # pad with a few numeric metrics if available
                for c in ["war","fwar","bwar","wrc_plus","ops_plus","pa","ip"]:
                    if c in cols and c not in show_cols:
                        show_cols.append(c)
                st.write(f"검색 결과: {len(hits)}")
                st.dataframe(hits[show_cols].head(200), use_container_width=True)
            else:
                st.info("이름 일부(예: *ohtani*, *judge* ) 또는 MLBAM ID를 입력하세요.")

# ========== Visuals ==========
with tab_visuals:
    st.subheader("Visuals")
    imgs = list_images(REP)
    pdfs = list_pdfs(REP)
    if not imgs and not pdfs:
        st.info("`output/reports/` 폴더에 이미지(PNG/JPG) 또는 PDF를 넣으면 자동으로 표시됩니다.")
    colA, colB = st.columns(2)
    shown = 0
    for p in imgs:
        try:
            col = colA if (shown % 2)==0 else colB
            col.image(p.read_bytes(), caption=p.name, use_container_width=True)
            shown += 1
        except (UnidentifiedImageError, OSError):
            continue
    if pdfs:
        st.divider()
        st.write("📄 PDF Reports")
        sel = st.selectbox("다운로드", [p.name for p in pdfs])
        st.download_button("다운로드", data=(REP/sel).read_bytes(), file_name=sel, mime="application/pdf")

# ========== Trade Simulator ==========
with tab_trade:
    st.subheader("Trade Simulator (portfolio mode)")
    st.caption("가치표 CSV가 있으면 실제 점수로 계산, 없으면 간이 방식 사용")

    value_df = build_value_table()
    if value_df is not None:
        # try to standardize
        cols = value_df.columns
        name_col = pick_col(cols, NAME_CANDS) or pick_col(cols, ["player","player_name"])
        id_col   = pick_col(cols, ID_CANDS)
        score_col = pick_col(cols, ["score","value","trade_score","fit_score","overall","rating"])
        st.write(f"가치표: rows={len(value_df)}, name={name_col}, id={id_col}, score={score_col}")

    left, right = st.columns(2)
    with left:
        out_txt = st.text_area("보내는 선수들 (줄바꿈 구분)", height=120, placeholder="ohtani\nsoto\n...")
    with right:
        in_txt = st.text_area("받는 선수들 (줄바꿈 구분)", height=120, placeholder="carroll\nacuna\n...")

    def parse_list(s):
        arr = [x.strip() for x in s.splitlines() if x.strip()]
        return arr[:50]

    if st.button("시뮬레이션 실행", type="primary"):
        outs = parse_list(out_txt)
        ins  = parse_list(in_txt)
        if value_df is not None and (name_col or id_col) and score_col:
            # sum scores by best matching on name (case-insensitive contains) or exact id
            def sum_score(names):
                tot = 0.0
                for n in names:
                    mask = pd.Series([False]*len(value_df))
                    if id_col and n.isdigit():
                        mask = mask | (value_df[id_col].astype(str)==n)
                    if name_col:
                        mask = mask | value_df[name_col].astype(str).str.contains(n, case=False, na=False)
                    sub = value_df.loc[mask]
                    if not sub.empty:
                        tot += float(np.nanmean(pd.to_numeric(sub[score_col], errors="coerce")))
                return tot
            out_score = sum_score(outs)
            in_score  = sum_score(ins)
        else:
            # fallback: simple heuristic (length-based dummy)
            out_score = sum([max(1, len(x)//3) for x in outs])
            in_score  = sum([max(1, len(x)//3) for x in ins])

        net = in_score - out_score
        verdict = "✅ 유리함" if net>0 else ("⚖️ 비슷함" if net==0 else "❌ 불리함")
        st.success(f"Trade Score: incoming {in_score:.2f} − outgoing {out_score:.2f} = **{net:.2f}** → {verdict}")

st.divider()
st.caption("Build keeps legacy visuals; adds robust player search & trade scoring with real data if available.")
PY

# 2) requirements.txt (사용자가 제공한 내용 그대로)
cat > "${ROOT}/requirements.txt" <<'REQ'
streamlit==1.39.0
duckdb==1.1.2
pandas==2.2.3
pyarrow==14.0.2
numpy==2.3.3
matplotlib==3.10.0
pillow==10.4.0
REQ

echo "✅ Wrote streamlit_app.py and requirements.txt"

# 선택: 자동 커밋/푸시 (현재 브랜치에)
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git add "${ROOT}/streamlit_app.py" "${ROOT}/requirements.txt"
  git commit -m "feat: drop-in streamlit app (portfolio + player search + trade) and pinned deps" || true
  git push || true
fi

echo "🎉 Done. Deploy/Rerun in Streamlit Cloud."
