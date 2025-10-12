# streamlit_app.py
# -*- coding: utf-8 -*-
import os, io, tarfile, json, textwrap
from pathlib import Path
import streamlit as st
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "output"
REP = OUT / "reports"
SUM = OUT / "summaries"
LOG = ROOT / "logs"
for p in (OUT, REP, SUM, LOG):
    p.mkdir(parents=True, exist_ok=True)

# -----------------------------
# 0) Bundle auto-fetch (optional)
# -----------------------------

def fetch_bundle_if_needed():
    url = os.environ.get("BUNDLE_URL") or st.secrets.get("BUNDLE_URL", "")
    if not url:
        return None
    marker = OUT / ".bundle_fetched"
    if marker.exists():
        return None
    try:
        import requests, tarfile, io
        st.info("Downloading artifacts bundle…")
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        buf = io.BytesIO(r.content)
        # output/ 하위 항목만 상대경로로 안전 추출
        with tarfile.open(fileobj=buf, mode="r:gz") as tf:
            def safe_members(members):
                for m in members:
                    name = m.name.lstrip("/").replace("..", "")
                    if name.startswith("output/"):  # output/만 허용
                        m.name = name
                        yield m
            tf.extractall(path=ROOT, members=safe_members(tf.getmembers()))
        marker.write_text("ok")
        st.success("Artifacts fetched into ./output")
    except Exception as e:
        st.warning(f"Bundle fetch skipped: {e}")

fetch_bundle_if_needed()
# -----------------------------
# 1) Safe loaders / helpers
# -----------------------------
def read_parquet_safe(p: Path, columns=None):
    if not p.exists():
        return pd.DataFrame()
    try:
        # Prefer pyarrow (NumPy 2.x compatibility), fallback to default
        return pd.read_parquet(p, columns=columns, engine="pyarrow")
    except Exception:
        try:
            return pd.read_parquet(p, columns=columns)
        except Exception as e:
            st.warning(f"Failed to read {p.name}: {e}")
            return pd.DataFrame()

def ensure_player_name(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    candidates = ["player_name","full_name","name","mlb_name","player","playerFullName"]
    for c in candidates:
        if c in df.columns:
            df["player_name"] = df[c].astype(str)
            return df
    if {"name_first","name_last"}.issubset(df.columns):
        df["player_name"] = (df["name_first"].astype(str) + " " + df["name_last"].astype(str)).str.strip()
        return df
    if "player_uid" in df.columns:
        df["player_name"] = df["player_uid"].astype(str)
        return df
    df["player_name"] = df.index.astype(str)
    return df

def read_qc():
    p = OUT / "full_system_validation.json"
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            return {}
    return {}
# -----------------------------
# 2) Load data
# -----------------------------
cards_en = read_parquet_safe(OUT/"player_cards_enriched_all_seq.parquet")
cards    = read_parquet_safe(OUT/"player_cards_all.parquet")
if cards_en.empty and cards.empty:
    st.warning("No player cards found under ./output/. Upload or fetch the bundle.")
base_cards = cards_en if not cards_en.empty else cards
base_cards = ensure_player_name(base_cards)

statcast = read_parquet_safe(OUT/"statcast_ultra_full_clean.parquet", columns=None)
statcast_preview = pd.DataFrame()
if not statcast.empty:
    preview_cols = [c for c in ["year","game_date","batter","pitcher","pitch_type","plate_x","plate_z"] if c in statcast.columns]
    statcast_preview = statcast[preview_cols].head(5000)

# -----------------------------
# 3) UI
# -----------------------------
st.set_page_config(page_title="Co-GM Core — MLB HF Final", layout="wide")
st.title("Co-GM Core — MLB HF Final Build")

tabs = st.tabs(["Overview", "Player Search", "Trade Evaluator", "Reports"])

# ---- Overview
with tabs[0]:
    st.subheader("Data Overview")
    rows_cards = len(base_cards)
    cols_cards = list(base_cards.columns)[:6]
    rows_sc = len(statcast)
    cols_sc = list(statcast.columns)[:6] if not statcast.empty else []
    st.table(pd.DataFrame([
        {"file":"player_cards_all.parquet", "rows": rows_cards, "columns": ", ".join(cols_cards)},
        {"file":"statcast_ultra_full_clean.parquet", "rows": rows_sc, "columns": ", ".join(cols_sc)},
    ]))
    qc = read_qc()
    with st.expander("QC & artifact presence (JSON)", expanded=True):
        st.code(json.dumps(qc, indent=2), language="json")
    st.subheader("Visuals (PNG)")
    pngs = [
        REP/"platoon_map.png",
        REP/"weakness_heatmap.png",
        REP/"ump_euz.png",
        REP/"explainable_attribution_topN.png",
    ]
    for p in pngs:
        if p.exists():
            st.image(str(p), caption=p.name, use_column_width=True)
        else:
            st.info(f"{p.name} (missing)")
# ---- Player Search
with tabs[1]:
    st.subheader("Player Search")
    q = st.text_input("Search player name:", value="")
    if q and not base_cards.empty:
        df = base_cards.copy()
        if "player_name" not in df.columns:
            df = ensure_player_name(df)
        try:
            hits = df[df["player_name"].str.contains(q, case=False, na=False)].copy()
        except KeyError:
            hits = pd.DataFrame()
        st.write(f"Matches: {len(hits)}")
        if not hits.empty:
            if "season" in hits.columns:
                hits = hits.sort_values(["player_name","season"], ascending=[True, False])
            st.dataframe(hits.head(200))
            trend_pdf = REP / "trend_cards_3y.pdf"
            if trend_pdf.exists():
                st.markdown(f"[Open trend_cards_3y.pdf]({trend_pdf.as_posix()})")
        else:
            st.info("No match.")
    elif q and base_cards.empty:
        st.warning("Cards table missing; cannot search.")

# ---- Trade Evaluator (lite)
with tabs[2]:
    st.subheader("Trade Evaluator (lite)")
    js = REP/"mock_trades_sample.json"
    if js.exists():
        try:
            data = json.loads(js.read_text())
            df = pd.DataFrame(data)
            st.dataframe(df)
            score_cols = [c for c in df.columns if "score" in c.lower() or "weight" in c.lower()]
            if score_cols:
                df["_score"] = df[score_cols].select_dtypes(include=[np.number]).sum(axis=1)
                st.write("Aggregate score (sum of numeric score/weight columns):")
                st.bar_chart(df.set_index(df.columns[0])["_score"])
        except Exception as e:
            st.warning(f"Trade sample parse failed: {e}")
    else:
        st.info("No mock trade file found (expected: output/reports/mock_trades_sample.json).")

# ---- Reports
with tabs[3]:
    st.subheader("Reports")
    pdfs = [REP/"auto_report_v2.pdf", REP/"legacy_report_v2.pdf", REP/"trend_cards_3y.pdf"]
    for p in pdfs:
        if p.exists():
            with open(p, "rb") as f:
                st.download_button(f"Download {p.name}", f, file_name=p.name, mime="application/pdf")
        else:
            st.info(f"{p.name} (missing)")

st.caption("Powered by real artifacts in ./output  •  If empty, set BUNDLE_URL as an env or Streamlit secret to auto-fetch.")
