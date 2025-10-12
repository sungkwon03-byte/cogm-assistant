#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MLB Portfolio App — All-in-One (English, Crash-Proof)
"""

import traceback
from pathlib import Path
import pandas as pd
import numpy as np
import streamlit as st

st.set_page_config(page_title="MLB Portfolio App", layout="wide")

ROOT = Path(".").resolve()
OUT = ROOT / "output"
REP = OUT / "reports"
SUM = OUT / "summaries"

F_CARDS = OUT / "player_cards_all.parquet"
F_CARDS_EN = OUT / "player_cards_enriched_all_seq.parquet"
F_STAT = OUT / "statcast_ultra_full_clean.parquet"

def safe_read_parquet(path, columns=None):
    try:
        return pd.read_parquet(path, columns=columns, engine="fastparquet")
    except Exception as e:
        st.warning(f"⚠️ Failed to read {path.name}: {e}")
        return pd.DataFrame()

def main():
    st.title("⚾ MLB Portfolio App (English Mode)")
    st.caption("Real Data • Visual Reports • Trade Analysis")

    try:
        tabs = st.tabs(["Overview", "Player Search", "Reports", "About"])
        with tabs[0]:
            st.subheader("Data Overview")
            info = []
            for f in [F_CARDS, F_STAT]:
                if f.exists():
                    df = safe_read_parquet(f)
                    info.append({
                        "file": f.name,
                        "rows": len(df),
                        "columns": list(df.columns)[:5]
                    })
            if info:
                st.dataframe(pd.DataFrame(info))
            else:
                st.error("No valid data files found in output/. Please check deployment bundle.")

        with tabs[1]:
            st.subheader("Player Search")
            q = st.text_input("Search player name:")
            if q and F_CARDS.exists():
                df = safe_read_parquet(F_CARDS)
                hits = df[df["player_name"].str.contains(q, case=False, na=False)]
                st.write(f"{len(hits)} result(s)")
                st.dataframe(hits.head(20))
            else:
                st.info("Enter a player name to begin searching.")

        with tabs[2]:
            st.subheader("Reports & Visuals")
            pngs = sorted(REP.glob("*.png"))
            pdfs = sorted(REP.glob("*.pdf"))
            if pngs:
                for p in pngs:
                    st.image(str(p), caption=p.name)
            if pdfs:
                for p in pdfs:
                    st.download_button(label=f"📄 Download {p.name}", file_name=p.name, data=p.read_bytes())
            if not (pngs or pdfs):
                st.info("No report files detected in output/reports/.")

        with tabs[3]:
            st.subheader("About This App")
            st.markdown("""
            **MLB Portfolio App** demonstrates real data integration for player evaluation, trade value,
            and visualization.  
            Built with Streamlit, Pandas, and Fastparquet.
            """)

    except Exception as e:
        st.error("❌ Unexpected error occurred:")
        st.code(traceback.format_exc())

if __name__ == "__main__":
    main()
