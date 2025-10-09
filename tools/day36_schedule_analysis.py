import argparse, pandas as pd, numpy as np
from math import radians, sin, cos, asin, sqrt

def haversine_km(a,b,c,d):
    R=6371.0; dlat=radians(c-a); dlon=radians(d-b)
    return 2*asin((sin(dlat/2)**2 + cos(radians(a))*cos(radians(c))*sin(dlon/2)**2)**0.5)*R

def sos_from_elo(x, lo=1300.0, hi=1700.0):
    x=np.clip(float(x), lo, hi); return (x-lo)/(hi-lo)

def rest_index(d):
    if d<=0: return 0.0
    if d==1: return 0.5
    return 1.0

def fatigue(dist_km, tz_diff, road_streak):
    d=min(dist_km/3000.0,1.0); tz=min(abs(tz_diff)/5.0,1.0); rs=min(road_streak/10.0,1.0)
    return 0.6*d+0.3*tz+0.1*rs

def run(schedule_csv, stadiums_csv, out_csv):
    sched=pd.read_csv(schedule_csv, parse_dates=["date"])
    st=pd.read_csv(stadiums_csv)  # venue,lat,lon,utc_offset_hours
    df=sched.merge(st, on="venue", how="left")
    df=df.sort_values(["team","date"]).reset_index(drop=True)

    res=[]; last_date={}
    for i,row in df.iterrows():
        team=row.team; date=row.date; elo=row.opponent_elo; ha=row.home_away
        rest_days=(date-last_date[team]).days-1 if team in last_date else 2
        last_date[team]=date

        # travel
        dist_km=0.0; tz=0.0
        prev=df[(df.team==team) & (df.date<date)].tail(1)
        if ha=="A" and len(prev):
            dist_km=haversine_km(prev.lat.values[0], prev.lon.values[0], row.lat, row.lon)
            tz=row.utc_offset_hours - prev.utc_offset_hours.values[0]

        road_streak=int(((df[(df.team==team) & (df.date<=date)].tail(5)).home_away=="A").sum())
        SoS=round(sos_from_elo(elo),3)
        Rest=rest_index(int(rest_days))
        Fat=round(fatigue(dist_km, tz, road_streak),3)
        CSI=round(0.5*SoS + 0.2*Rest + 0.3*(1-Fat),3)

        res.append(dict(date=date.date(), team=team, opponent=row.opponent, home_away=ha,
                        venue=row.venue, opponent_elo=elo, dist_km=round(dist_km,1),
                        tz_diff=float(tz), rest_days=int(rest_days), SoS=SoS,
                        RestIndex=Rest, FatigueScore=Fat, CSI=CSI))
    out=pd.DataFrame(res); out.to_csv(out_csv, index=False)
    print(f"[Day36] done -> {out_csv}  rows={len(out)}")

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--schedule", required=True)
    ap.add_argument("--stadiums", required=True)
    ap.add_argument("--out", required=True)
    a=ap.parse_args()
    run(a.schedule, a.stadiums, a.out)
