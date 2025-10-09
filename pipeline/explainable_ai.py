import pandas as pd, numpy as np
from pathlib import Path
OUT=Path.cwd()/'output'; OUT.mkdir(exist_ok=True)
f=OUT/'statcast_features_player_year.csv'
if not f.exists():
    pd.DataFrame(columns=['feature','coef']).to_csv(OUT/'explainable_feature_attrib.csv', index=False)
    print("[72][WARN] no statcast_features -> explainable_feature_attrib.csv (empty)"); raise SystemExit
df=pd.read_csv(f, low_memory=False)
y=pd.to_numeric(df['xwOBA'], errors='coerce')
X=df[['whiff_rate','chase_rate','z_contact_rate','o_contact_rate','barrel_rate','hardhit_rate','avg_ev']].copy()
for c in X.columns: X[c]=pd.to_numeric(X[c], errors='coerce')
Z=X.fillna(X.median(numeric_only=True))
Z=np.c_[np.ones(len(Z)), Z.values]  # bias
mask=np.isfinite(y.values)
y=y.values[mask]; Z=Z[mask]
coef, *_ = np.linalg.lstsq(Z, y, rcond=None)
cols=['bias']+X.columns.tolist()
pd.DataFrame({'feature':cols,'coef':coef}).to_csv(OUT/'explainable_feature_attrib.csv', index=False)
print("[72] explainable_feature_attrib.csv features=", len(cols))
