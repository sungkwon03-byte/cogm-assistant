#!/usr/bin/env bash
set +e
ok(){ [ -s "$1" ]; }
echo "[DOCTOR] start"
printf '{"exists":{"cards":%s,"statcast":%s,"id_map":%s}}\n' \
 $(ok output/player_cards_all.parquet && echo true || echo false) \
 $(ok output/statcast_ultra_full_clean.parquet && echo true || echo false) \
 $(ok output/id_map.csv && echo true || echo false)
echo "[DOCTOR] done"
