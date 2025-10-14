from pybaseball import statcast
import pandas as pd, sys
date = sys.argv[1] if len(sys.argv)>1 else "2025-04-01"
df = statcast(start_dt=date, end_dt=date)  # pitch/BBE 레벨
# 필요한 컬럼만 정리 → our ingest 스키마에 맞춤
keep = {
  "game_pk":"game_pk",
  "batter":"mlb_id",
  "launch_speed":"EV",
  "launch_angle":"LA",
  "estimated_woba_using_speedangle":"xwOBA",
  "pitch_type":"pitch_type",
  "events":"events"
}
out = pd.DataFrame({dst: df[src] if src in df.columns else None for src,dst in keep.items()})
# is_bip: 타구 발생 여부
out["is_bip"] = out["events"].notna() & (out["events"].astype(str)!="")
out.to_csv("data/statcast.csv", index=False)
print(f"[fetch] data/statcast.csv rows={len(out)}")
