from fastapi import FastAPI
from pathlib import Path

app = FastAPI()

@app.get("/")
def root():
    return {"ok": True, "mode": "fallback"}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/check")
def check():
    return {
        "cards": Path("output/player_cards_all.parquet").exists(),
        "statcast": Path("output/statcast_ultra_full_clean.parquet").exists(),
        "id_map": Path("output/id_map.csv").exists(),
    }
