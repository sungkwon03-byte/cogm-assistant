import math, pandas as pd
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'
parks = {
 "LAD": (34.0739,-118.2400),"SFG": (37.7786,-122.3893),"SDP": (32.7073,-117.1573),
 "NYA": (40.8296,-73.9262),"NYN": (40.7571,-73.8458),"BOS": (42.3467,-71.0972),
 "ATL": (33.8907,-84.4677),"CHN": (41.9484,-87.6553),"CHA": (41.8301,-87.6339),
 "HOU": (29.7572,-95.3556),"TEX": (32.7473,-97.0842),"PHI": (39.9057,-75.1665),
 "TOR": (43.6414,-79.3894),"BAL": (39.2839,-76.6217),"WSN": (38.8730,-77.0074),
 "SLN": (38.6226,-90.1928),"CIN": (39.0970,-84.5070),"MIL": (43.0280,-87.9712),
 "MIN": (44.9817,-93.2776),"DET": (42.3390,-83.0485),"CLE": (41.4962,-81.6852),
 "PIT": (40.4469,-80.0057),"MIA": (25.7783,-80.2209),"TBA": (27.7680,-82.6534),
 "ARI": (33.4455,-112.0667),"COL": (39.7559,-104.9942),"SEA": (47.5914,-122.3325),
 "OAK": (37.7516,-122.2005),"LAA": (33.8003,-117.8827),"KCR": (39.0516,-94.4803)
}
from math import radians, sin, cos, asin, sqrt
def hav(a,b):
    R=6371.0
    lat1,lon1=a; lat2,lon2=b
    dlat=radians(lat2-lat1); dlon=radians(lon2-lon1)
    x=sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    return 2*R*asin(sqrt(x))
rows=[]
teams=sorted(parks.keys())
for t1 in teams:
    for t2 in teams:
        d=0.0 if t1==t2 else hav(parks[t1], parks[t2])
        rows.append({'from':t1,'to':t2,'km':d,'fatigue_idx': d/1500.0})
pd.DataFrame(rows).to_csv(OUT/'park_distance_matrix.csv', index=False)
print("[OK] park_distance_matrix.csv")
