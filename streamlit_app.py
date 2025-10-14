# streamlit_app.py
# 포트폴리오 데모 완전본:
# - 실데이터(Statcast/Lahman 카드) 연동
# - Player Search/조회
# - Trade Evaluator
# - Reports/Visuals 안전 뷰어
# - QC/Logs + Self-Heal

from pathlib import Path
import io, json, base64
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(".")
OUT = ROOT / "output"
REP = OUT / "reports"
SUM = OUT / "summaries"
LOGD = ROOT / "logs"
for p in (OUT, REP, SUM, LOGD): p.mkdir(parents=True, exist_ok=True)

P_STAT = OUT / "statcast_ultra_full_clean.parquet"
P_CARDS = OUT / "player_cards_all.parquet"
P_CARDS_SEQ = OUT / "player_cards_enriched_all_seq.parquet"
NEED = [
    SUM / "platoon_split.csv",
    REP / "platoon_map.png",
    SUM / "weakness_heatmap_matrix.csv",
    REP / "weakness_heatmap.png",
    REP / "trend_cards_3y.pdf",
    SUM / "euz_umpire_impact.csv",
    REP / "ump_euz.png",
    REP / "explainable_attribution_topN.png",
]

st.set_page_config(page_title="Co-GM Assistant — Portfolio", layout="wide")
st.title("🧠 Co-GM Assistant — Portfolio Demo")
st.caption("Statcast(2015–), Lahman(1901–) 기반 실데이터 • 리포트/비주얼 • 검색/조회 • 트레이드 간이점수 • QC/로그 + Self-Heal")

@st.cache_data(show_spinner=False)
def read_any_table(path: Path):
    if not path.exists(): return None
    try:
        if path.suffix.lower()==".parquet":
            return pd.read_parquet(path)
    except Exception: pass
    try:
        return pd.read_csv(path)
    except Exception:
        return None

def pick_col(df, cands):
    if df is None or df.empty: return None
    cols = list(df.columns)
    for c in cands:
        if c in cols: return c
    lower = {c.lower(): c for c in cols}
    for c in cands:
        if c.lower() in lower: return lower[c.lower()]
    return None

def safe_image_list(folder: Path):
    if not folder.exists(): return []
    exts = {".png",".jpg",".jpeg",".gif"}
    files = []
    for p in sorted(folder.glob("*")):
        if p.is_file() and p.suffix.lower() in exts and p.stat().st_size>0:
            try:
                with open(p,"rb") as f: head=f.read(16)
                if head.startswith(b"\x89PNG") or head.startswith(b"\xff\xd8") or head[:3]==b"GIF":
                    files.append(p)
            except Exception: pass
    return files

def safe_pdf_list(folder: Path):
    if not folder.exists(): return []
    files = []
    for p in sorted(folder.glob("*.pdf")):
        try:
            with open(p,"rb") as f: head=f.read(5)
            if head==b"%PDF-": files.append(p)
        except Exception: pass
    return files

def qc_json():
    qc = (OUT/"full_system_validation.json")
    if qc.exists():
        try: return json.loads(qc.read_text())
        except Exception: return {}
    return {}

def vis_status_json():
    s = (SUM/"visuals_final_status.json")
    if s.exists():
        try: return json.loads(s.read_text())
        except Exception: return {}
    return {}

def ensure_visuals_from_real():
    stat = read_any_table(P_STAT)
    cards = read_any_table(P_CARDS)
    seq = read_any_table(P_CARDS_SEQ)
    try:
        if not (SUM/"platoon_split.csv").exists() or not (REP/"platoon_map.png").exists():
            if stat is not None and not stat.empty:
                hb = pick_col(stat,["batter_hand","stand","bats"])
                hp = pick_col(stat,["pitcher_throws","p_throws","throws"])
                y = pick_col(stat,["year","game_year"])
                if y is None and "game_date" in stat.columns:
                    stat["year"]=pd.to_datetime(stat["game_date"],errors="coerce").dt.year
                    y="year"
                if hb and hp and y:
                    grp = stat.groupby([y,hb,hp],dropna=True).size().reset_index(name="PA")
                    grp.to_csv(SUM/"platoon_split.csv",index=False)
                    fig=plt.figure(figsize=(6,3))
                    plt.bar(range(len(grp)),grp["PA"]);plt.tight_layout()
                    plt.savefig(REP/"platoon_map.png",dpi=150);plt.close(fig)
    except Exception:
        fig=plt.figure(figsize=(6,3));plt.title("Platoon (no data)")
        plt.tight_layout();plt.savefig(REP/"platoon_map.png",dpi=150);plt.close(fig)
        pd.DataFrame({"note":["no-data"]}).to_csv(SUM/"platoon_split.csv",index=False)

with st.sidebar:
    st.header("📦 Data Status")
    st.write(f"• Statcast: {'✅' if P_STAT.exists() else '❌'}")
    st.write(f"• Cards: {'✅' if P_CARDS.exists() else '❌'}")
    st.write(f"• Cards (seq): {'✅' if P_CARDS_SEQ.exists() else '❌'}")
    if st.button("🔧 Self-Heal"):
        ensure_visuals_from_real()
        st.success("자가복구 완료. 새로고침하세요.")

tab_home,tab_search,tab_trade,tab_reports,tab_qc = st.tabs(
    ["🏠 Overview","🔎 Player Search","💱 Trade Evaluator","🖼 Reports & Visuals","🧾 QC & Logs"]
)

with tab_home:
    st.markdown("""
- Sidebar에서 데이터 확인/자가복구  
- Player Search: 이름/ID 자동매핑  
- Trade Evaluator: wRC+/EV/PA 기반 간이점수  
- Reports & Visuals: 안전 렌더링  
- QC & Logs: 검증/로그 상태
""")

with tab_search:
    cards=read_any_table(P_CARDS)
    base=read_any_table(P_CARDS_SEQ) or cards
    if base is None or base.empty:
        st.info("카드 데이터가 없습니다.")
    else:
        name_col=pick_col(base,["player_name","full_name","name","mlb_name"])
        id_col=pick_col(base,["player_id","mlb_id"])
        q=st.text_input("선수명","")
        if q.strip():
            mask=base[name_col].astype(str).str.contains(q,case=False,na=False)
            hit=base[mask]
            if hit.empty: st.warning("검색 결과 없음.")
            else: st.dataframe(hit.head(50),use_container_width=True)

with tab_trade:
    seq=read_any_table(P_CARDS_SEQ) or read_any_table(P_CARDS)
    if seq is None or seq.empty:
        st.info("데이터 없음.")
    else:
        name_col=pick_col(seq,["player_name","full_name","name"])
        id_col=pick_col(seq,["player_id","mlb_id"])
        a=st.text_input("Team A 선수","")
        b=st.text_input("Team B 선수","")
        def score(row):
            wrc=pd.to_numeric(row.get("wRC_plus",np.nan),errors="coerce")
            ev=pd.to_numeric(row.get("EV",np.nan),errors="coerce")
            pa=pd.to_numeric(row.get("PA",np.nan),errors="coerce")
            return 0.6*(wrc if pd.notna(wrc) else 100)+0.3*(ev if pd.notna(ev) else 88)+0.1*(np.log1p(pa) if pd.notna(pa) else 3)
        if st.button("🔍 평가"):
            def pick(df,q):
                if not q.strip(): return None
                if name_col: hit=df[df[name_col].astype(str).str.contains(q,case=False,na=False)]
                else: hit=df.head(0)
                return hit.head(1)
            A=pick(seq,a);B=pick(seq,b)
            if A is None or B is None: st.warning("양쪽 선수 확인 필요.")
            else:
                sA=score(A.iloc[0]);sB=score(B.iloc[0])
                st.write(f"A:{sA:.1f} / B:{sB:.1f}")
                if sA>sB: st.success("→ A가 이득")
                elif sA<sB: st.success("→ B가 이득")
                else: st.info("→ 대등")

with tab_reports:
    imgs=safe_image_list(REP);pdfs=safe_pdf_list(REP)
    if imgs:
        cols=st.columns(2)
        for i,img in enumerate(imgs):
            with cols[i%2]:
                st.image(str(img),caption=img.name,use_container_width=True)
    else: st.info("표시할 이미지 없음.")
    if pdfs:
        for p in pdfs:
            with open(p,"rb") as f:data=f.read()
            st.download_button(label=f"📄 {p.name}",data=data,file_name=p.name,mime="application/pdf")
    else: st.info("표시할 PDF 없음.")

with tab_qc:
    st.json(qc_json() or {"note":"no qc file"})
    st.json(vis_status_json() or {"note":"no visuals"})
    logs=sorted(LOGD.glob("*.log"))
    if logs:
        for lg in logs:
            st.markdown(f"**{lg.name}**")
            st.code(lg.read_text()[-2000:],language="bash")
    else: st.info("logs/*.log 없음")
