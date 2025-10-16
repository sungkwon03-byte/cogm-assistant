import streamlit as st
import pandas as pd
from app.lib.name_resolver import search, resolve_names
import pyarrow as pa, pyarrow.parquet as pq

st.set_page_config(page_title="Trade Builder (Names)", layout="wide")
st.title("Trade Builder (Names)")

@st.cache_data
def load_cards():
    return pd.read_parquet("output/player_cards_all.parquet")

cards = load_cards()
name_q = st.text_input("선수 검색 (쉼표로 여러 명)", placeholder="mike trout, mookie betts, ...")

colA, colB = st.columns(2)
with colA:
    st.subheader("Package A")
    a_q = st.text_input("A 패키지: 이름 목록", value=name_q)
with colB:
    st.subheader("Package B")
    b_q = st.text_input("B 패키지: 이름 목록", value="")

def tokenize(s:str):
    return [x.strip() for x in (s or "").split(",") if x.strip()]

def to_ids(name_list):
    ids, diag = resolve_names(name_list)
    return ids, pd.DataFrame(diag)

if st.button("평가하기", type="primary"):
    A_names, B_names = tokenize(a_q), tokenize(b_q)
    A_ids, A_diag = to_ids(A_names)
    B_ids, B_diag = to_ids(B_names)
    st.write("### 변환 결과")
    st.write("A 변환", A_diag)
    st.write("B 변환", B_diag)

    # cards에서 id 컬럼 추정 (가장 그럴듯한 식별자 찾기)
    id_col = None
    for c in ["player_id","bbref_id","retro_id","retroID","mlbam_id","mlbam","mlb_id","key_mlbam","fg_id"]:
        if c in cards.columns:
            id_col = c; break
    if id_col is None:
        st.error("cards에서 ID 컬럼을 찾지 못했습니다."); st.stop()

    A_df = cards[cards[id_col].astype(str).isin(A_ids)]
    B_df = cards[cards[id_col].astype(str).isin(B_ids)]
    if A_df.empty and B_df.empty:
        st.warning("양쪽 모두 매칭된 선수가 없습니다.")
    else:
        metric_cols = [c for c in ["wOBA","wRC+","WAR","OPS","OBP","SLG","EV","Hard%"] if c in cards.columns]
        def summarize(df):
            if df.empty: return pd.DataFrame()
            out = pd.DataFrame({
                "N":[len(df)],
                **{f"avg_{m}":[df[m].astype(float).mean()] for m in metric_cols}
            })
            return out
        st.write("#### Package A 요약"); st.dataframe(A_df[[id_col]+[c for c in metric_cols if c in A_df.columns]].head(50))
        st.table(summarize(A_df))
        st.write("#### Package B 요약"); st.dataframe(B_df[[id_col]+[c for c in metric_cols if c in B_df.columns]].head(50))
        st.table(summarize(B_df))

st.divider()
st.caption("이름→ID 자동 변환(퍼지 매칭). 동명이인은 팀/데뷔연도 정보가 있을 경우 우선 반영.")
