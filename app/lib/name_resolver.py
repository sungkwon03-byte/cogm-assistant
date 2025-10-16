import unicodedata,re,pandas as pd
def normalize_name(name:str)->str:
    if not isinstance(name,str): return ""
    name=unicodedata.normalize("NFKC",name).strip()
    return re.sub(r"[^A-Za-zÀ-ÿ'\\- ]","",name).lower()
def find_player(df:pd.DataFrame,name:str):
    n=normalize_name(name)
    return df[df['name_normalized'].str.lower()==n]
