#!/usr/bin/env python3
import re, pandas as pd
from unidecode import unidecode
def normalize_name(s: str) -> str:
    if pd.isna(s): return ""
    s = str(s); s = unidecode(s)
    s = s.replace(".", " ")
    s = re.sub(r"[^A-Za-z0-9\s\-']", " ", s)
    s = re.sub(r"\s+", " ", s).strip().upper()
    return s
def add_norm_name_column(df: pd.DataFrame, src_col="Name", out_col="name_norm") -> pd.DataFrame:
    if src_col not in df.columns:
        df[out_col] = ""
        return df
    df[out_col] = df[src_col].map(normalize_name)
    return df
