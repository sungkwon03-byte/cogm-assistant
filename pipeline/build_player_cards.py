#!/usr/bin/env python3
import os, glob, pandas as pd, numpy as np, datetime
from importlib.util import find_spec

def read_mart(season:int):
    files = []
    for lg in ["mlb","kbo","milb"]:
        files += glob.glob(f"mart/{lg}_{season}_players.csv")
    if not files:
        files = glob.glob("mart/*players.csv")
    if not files:
        raise FileNotFoundError("mart/*players.csv 를 찾지 못했습니다. 인제스트를 먼저 실행하세요.")
    frames = []
    for f in files:
        try:
            frames.append(pd.read_csv(f))
        except Exception:
            pass
    if not frames:
        raise FileNotFoundError("mart 내 CSV를 읽지 못했습니다.")
    return pd.concat(frames, ignore_index=True, sort=False)

def main():
    os.makedirs("output", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    season = int(os.environ.get("SEASON", "2024"))

    df = read_mart(season)
    df["last_update_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    out_log = f"logs/player_cards_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.log"
    with open(out_log, "w", encoding="utf-8") as f:
        f.write(f"[OK] player_cards generated at {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n")

    out_path = "output/player_cards.csv"
    df.to_csv(out_path, index=False)
    print(f"[OK] player_cards saved to {out_path} (rows={len(df)})")

    if find_spec("tabulate") is not None:
        df.head(10).to_markdown("output/player_cards_sample.md", index=False)
        print("[OK] sample -> output/player_cards_sample.md")
    else:
        df.head(10).to_csv("output/player_cards_sample.csv", index=False)
        print("[OK] sample -> output/player_cards_sample.csv")

if __name__ == "__main__":
    main()
