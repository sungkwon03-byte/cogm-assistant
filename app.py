#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ---------- Imports ----------
import shutil
import tarfile
import io
import os, io, json, tarfile
from pathlib import Path
ROOT = Path(__file__).resolve().parent
OUT = ROOT / "output"
REP = OUT / "reports"
SUM = OUT / "summaries"
LOG = ROOT / "logs"
for _p in (OUT, REP, SUM, LOG):
    _p.mkdir(parents=True, exist_ok=True)
import pandas as pd
import numpy as np
import streamlit as st

ROOT = Path('.').resolve()
OUT  = ROOT / 'output'
REP  = OUT / 'reports'
SUM  = OUT / 'summaries'
LOG  = ROOT / 'logs'
for _p in (OUT, REP, SUM, LOG):
    _p.mkdir(parents=True, exist_ok=True)


# ---------- Streamlit page config FIRST ----------
st.set_page_config(page_title="Co-GM Core — MLB HF Final (EN)", layout="wide")

# ---------- Paths ----------
ROOT = Path(__file__).resolve().parent
OUT  = ROOT / "output"
REP  = OUT / "reports"
SUM  = OUT / "summaries"
LOG  = ROOT / "logs"
for p in (OUT, REP, SUM, LOG):
    p.mkdir(parents=True, exist_ok=True)

# ---------- Bundle fetch (fully sandboxed; never crash) ----------


def fetch_bundle_if_needed():
    url = os.environ.get("BUNDLE_URL") or st.secrets.get("BUNDLE_URL", "")
    if not url:
        return
    marker = OUT / ".bundle_fetched"
    if marker.exists():
        return
    try:
        import requests
        with st.spinner("Downloading artifacts bundle…"):
            r = requests.get(url, timeout=120, allow_redirects=True)
            r.raise_for_status()
            buf = io.BytesIO(r.content)

            cache = ROOT / ".cache_bundle"
            if cache.exists():
                shutil.rmtree(cache, ignore_errors=True)
            cache.mkdir(parents=True, exist_ok=True)

            with tarfile.open(fileobj=buf, mode="r:gz") as tf:
                tf.extractall(path=cache)

            # deepest 'output/' in the bundle → sync to ./output
            candidates = [pp for pp in cache.rglob("output") if pp.is_dir()]
            if not candidates:
                raise RuntimeError("No 'output/' directory found in bundle")
            src_out = max(candidates, key=lambda pp: len(str(pp)))
            for item in src_out.rglob("*"):
                rel = item.relative_to(src_out)
                dst = OUT / rel
                if item.is_dir():
                    dst.mkdir(parents=True, exist_ok=True)
                else:
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    dst.write_bytes(item.read_bytes())
            marker.write_text("ok")
        st.success("Artifacts fetched → ./output")
    except Exception as e:
        st.warning(f"Bundle fetch skipped: {e}")
    if not url:
        try:
            url = st.secrets.get("BUNDLE_URL", "")
        except Exception:
            url = ""
    if not url:
        return
    marker = OUT / ".bundle_fetched"
    if marker.exists():
        return
    try:
        with st.spinner("Downloading artifacts bundle…"):
            r = requests.get(url, timeout=120, allow_redirects=True)
            r.raise_for_status()
            buf = io.BytesIO(r.content)

            cache = ROOT / ".cache_bundle"
            if cache.exists():
                shutil.rmtree(cache)
            cache.mkdir(parents=True, exist_ok=True)

            with tarfile.open(fileobj=buf, mode="r:gz") as tf:
                tf.extractall(path=cache)

            # pick the deepest 'output/' dir in bundle
            candidates = [pp for pp in cache.rglob("output") if pp.is_dir()]
            if not candidates:
                raise RuntimeError("No output/ directory found in bundle")
            src_out = max(candidates, key=lambda pp: len(str(pp)))

            for pp in src_out.rglob("*"):
                rel = pp.relative_to(src_out)
                dst = OUT / rel
                if pp.is_dir():
                    dst.mkdir(parents=True, exist_ok=True)
                else:
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    dst.write_bytes(pp.read_bytes())

            marker.write_text("ok", encoding="utf-8")
        st.success("Artifacts fetched → ./output")
    except Exception as e:
        # never crash; just warn
        st.warning(f"Bundle fetch skipped: {e}")


# 절대 예외 전파하지 않도록 2중 보호
try:
    fetch_bundle_if_needed()
except Exception as _e:
    st.info(f"Bundle disabled: {_e}")

# ---------- IO helpers ----------
def read_parquet_safe(path: Path, columns=None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    # pyarrow 우선 → 실패 시 기본/fastparquet 시도
    for engine in ("pyarrow", None, "fastparquet"):
        try:
            return pd.read_parquet(path, columns=columns, engine=engine) if engine else pd.read_parquet(path, columns=columns)
        except Exception:
            continue
    st.warning(f"Failed to read {path.name}")
    return pd.DataFrame()

def ensure_player_name(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    cands = ["player_name","full_name","name","mlb_name","player","playerFullName"]
    for c in cands:
        if c in df.columns:
            df["player_name"] = df[c].astype(str)
            return df
    if {"name_first","name_last"}.issubset(df.columns):
        df["player_name"] = (df["name_first"].astype(str)+" "+df["name_last"].astype(str)).str.strip()
        return df
    if "player_uid" in df.columns:
        df["player_name"] = df["player_uid"].astype(str)
        return df
    df["player_name"] = df.index.astype(str)
    return df

def read_qc() -> dict:
    p = OUT / "full_system_validation.json"
    if p.exists():
        try: return json.loads(p.read_text())
        except Exception: return {}
    return {}

# ---------- Load data (never crash) ----------
cards_en = read_parquet_safe(OUT/"player_cards_enriched_all_seq.parquet")
cards    = read_parquet_safe(OUT/"player_cards_all.parquet")
cards = normalize_player_name_column(cards)
base_cards = cards_en if not cards_en.empty else cards
base_cards = ensure_player_name(base_cards)

statcast = read_parquet_safe(OUT/"statcast_ultra_full_clean.parquet")
statcast_preview = pd.DataFrame()
if not statcast.empty:
    cols = [c for c in ["year","game_date","batter","pitcher","pitch_type","plate_x","plate_z"] if c in statcast.columns]
    statcast_preview = statcast[cols].head(5000) if cols else pd.DataFrame()

# ---------- UI ----------
st.title("Co-GM Core — MLB HF Final (EN) Build")

tabs = st.tabs(["Overview", "Player Search", "Trade Evaluator", "Reports"])

# Overview
with tabs[0]:
    st.subheader("Data Overview")
    st.table(pd.DataFrame([
        {"file":"player_cards_all.parquet",      "rows": len(base_cards), "columns": ", ".join(list(base_cards.columns)[:6])},
cards = normalize_player_name_column(cards)
        {"file":"statcast_ultra_full_clean.parquet", "rows": len(statcast),   "columns": ", ".join(list(statcast.columns)[:6]) if not statcast.empty else ""},
    ]))
    with st.expander("QC & artifact presence (JSON)", expanded=True):
        st.code(json.dumps(read_qc(), indent=2), language="json")
    st.subheader("Visuals (PNG)")
    for p in [REP/"platoon_map.png", REP/"weakness_heatmap.png", REP/"ump_euz.png", REP/"explainable_attribution_topN.png"]:
        if p.exists(): st.image(str(p), caption=p.name, use_column_width=True)
        else:          st.info(f"{p.name} (missing)")

# Player Search
with tabs[1]:
    st.subheader("Player Search")
    q = st.text_input("Search player name:", value="")
    if q and not base_cards.empty:
        df = base_cards.copy()
        if "player_name" not in df.columns:
            df = ensure_player_name(df)
        hits = pd.DataFrame()
        try:
            hits = df[df["player_name"].str.contains(q, case=False, na=False)].copy()
        except Exception:
            pass
        st.write(f"Matches: {len(hits)}")
        if not hits.empty:
            if "season" in hits.columns:
                hits = hits.sort_values(["player_name","season"], ascending=[True, False])
            st.dataframe(hits.head(200), use_container_width=True)
            tp = REP/"trend_cards_3y.pdf"
            if tp.exists():
                st.markdown(f"[Open {tp.name}]({tp.as_posix()})")
        else:
            st.info("No match.")
    elif q and base_cards.empty:
        st.warning("Cards table missing; cannot search.")

# Trade (lite)
with tabs[2]:
    st.subheader("Trade Evaluator (lite)")
    js = REP/"mock_trades_sample.json"
    if js.exists():
        try:
            data = json.loads(js.read_text()); df = pd.DataFrame(data)
            st.dataframe(df, use_container_width=True)
            cols = [c for c in df.columns if "score" in c.lower() or "weight" in c.lower()]
            if cols:
                df["_score"] = df[cols].select_dtypes(include=[np.number]).sum(axis=1)
                st.write("Aggregate score (sum of numeric score/weight columns):")
                st.bar_chart(df.set_index(df.columns[0])["_score"])
        except Exception as e:
            st.warning(f"Trade sample parse failed: {e}")
    else:
        st.info("No mock trade file found (expected: output/reports/mock_trades_sample.json).")

# Reports
with tabs[3]:
    st.subheader("Reports")
    for p in [REP/"auto_report_v2.pdf", REP/"legacy_report_v2.pdf", REP/"trend_cards_3y.pdf"]:
        if p.exists():
            with open(p, "rb") as f:
                st.download_button(f"Download {p.name}", f, file_name=p.name, mime="application/pdf")
        else:
            st.info(f"{p.name} (missing)")

st.caption("Runs on real artifacts in ./output • If empty, set BUNDLE_URL (secret or env) to auto-fetch.")


NAME_CANDIDATES = ["player_name","playerName","name","full_name","player","PlayerName"]

def normalize_player_name_column(df):
    import pandas as pd
    if df is None or getattr(df, "empty", True):
        return pd.DataFrame() if df is None else df
    for c in NAME_CANDIDATES:
        if c in df.columns:
            if c != "player_name":
                df = df.rename(columns={c:"player_name"})
            break
    else:
        if "player_uid" in df.columns:
            df["player_name"] = df["player_uid"].astype(str)
        else:
            df["player_name"] = ""
    df["player_name"] = df["player_name"].fillna("").astype(str)
    return df

@st.cache_data(show_spinner=False)
def load_cards():
    import pandas as pd
    paths = [OUT / "player_cards_enriched_all_seq.parquet", OUT / "player_cards_all.parquet"]
    df = pd.DataFrame()
    for path in paths:
        if path.exists():
            try:
                df = pd.read_parquet(path, engine="pyarrow")
            except Exception:
                try:
                    df = pd.read_parquet(path)
                except Exception:
                    df = pd.DataFrame()
            if not df.empty:
                break
    return normalize_player_name_column(df)

