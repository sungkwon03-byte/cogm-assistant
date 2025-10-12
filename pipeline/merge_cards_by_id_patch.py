#!/usr/bin/env python3
# attach_cards_by_id_patch — batter/player_id 병합 자동화
import pandas as pd
from pathlib import Path

SC_PATH = Path("output/statcast_ultra_full_clean.parquet")
CARDS_PATH = Path("output/player_cards_enriched_all_seq.parquet")
OUT_PATH = Path("output/statcast_with_cards.parquet")

if not SC_PATH.exists():
    raise FileNotFoundError(f"missing {SC_PATH}")
if not CARDS_PATH.exists():
    raise FileNotFoundError(f"missing {CARDS_PATH}")

sc = pd.read_parquet(SC_PATH)
cards = pd.read_parquet(CARDS_PATH)

# 사용할 컬럼 존재 확인
sc_id = "batter" if "batter" in sc.columns else None
card_id = "player_id" if "player_id" in cards.columns else None

if sc_id and card_id:
    cols = ["player_id","player_name"]
    for extra in ["bats","throws"]:
        if extra in cards.columns:
            cols.append(extra)
    merged = sc.merge(cards[cols], left_on=sc_id, right_on=card_id, how="left")
    print(f"[OK] merged statcast({len(sc)}) + cards({len(cards)}) → {len(merged)} rows")
else:
    raise KeyError(f"missing columns: batter({sc_id}), player_id({card_id})")

# 병합 결과 저장
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
merged.to_parquet(OUT_PATH, index=False)
print(f"[DONE] saved to {OUT_PATH}")
print(merged.head(5))
