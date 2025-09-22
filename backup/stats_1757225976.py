from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import sqlite3, os

DB_PATH = os.path.abspath("data/lahman.sqlite")
router = APIRouter()

class AnswerEnvelope(BaseModel):
    answer: str
    analysis: dict
    evidence: list
    actions: list

@router.get("/get_player_stats", response_model=AnswerEnvelope)
def get_player_stats(
    name: str = Query(..., description="Player full name, e.g., Shohei Ohtani"),
    season: int = Query(..., ge=1871, le=2100, description="Season year"),
):
    if not os.path.exists(DB_PATH):
        raise HTTPException(500, "lahman.sqlite missing")

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # 후보 찾기
    cur.execute("""
        SELECT p.playerID, p.nameFirst||' '||p.nameLast AS full
        FROM People p
        WHERE (p.nameFirst||' '||p.nameLast) LIKE ?
    """, (f"%{name}%",))
    cands = [dict(r) for r in cur.fetchall()]
    if not cands:
        raise HTTPException(404, f"No player LIKE '{name}'")

    # 시즌 기록 후보 선택
    picked = None
    for c in cands:
        cur.execute("SELECT * FROM Batting WHERE playerID=? AND yearID=?", (c["playerID"], season))
        row = cur.fetchone()
        if row:
            picked = (c, row)
            break
    if not picked:
        best = None
        for c in cands:
            cur.execute("SELECT * FROM Batting WHERE playerID=? ORDER BY yearID DESC LIMIT 1", (c["playerID"],))
            r = cur.fetchone()
            if r and (best is None or r["yearID"] > best[1]["yearID"]):
                best = (c, r)
        c, r = best if best else (cands[0], None)
        yr = r["yearID"] if r else "N/A"
        raise HTTPException(404, f"No batting for {c['full']} in {season}. Try season {yr}")

    c, row = picked
    AB, H, _2, _3, HR = row["AB"], row["H"], row["2B"], row["3B"], row["HR"]
    BB, HBP, SF = row["BB"], row["HBP"], row["SF"]
    sng = max(H - _2 - _3 - HR, 0)
    AVG = (H/AB) if AB>0 else 0.0
    OBP = ((H + BB + HBP) / (AB + BB + HBP + SF)) if (AB + BB + HBP + SF) > 0 else 0.0
    TB  = sng + 2*_2 + 3*_3 + 4*HR
    SLG = (TB/AB) if AB>0 else 0.0
    OPS = OBP + SLG

    analysis = {
        "player": c["full"], "season": season,
        "AB": AB, "H": H, "2B": _2, "3B": _3, "HR": HR,
        "BB": BB, "HBP": HBP, "SF": SF,
        "AVG": round(AVG,3), "OBP": round(OBP,3),
        "SLG": round(SLG,3), "OPS": round(OPS,3),
    }
    evidence = [{"src":"Lahman(baseballdatabank)","url":"https://github.com/chadwickbureau/baseballdatabank","snap_ts":"local"}]
    return AnswerEnvelope(
        answer=f"{c['full']} {season} slash {analysis['AVG']}/{analysis['OBP']}/{analysis['SLG']} (OPS {analysis['OPS']})",
        analysis=analysis, evidence=evidence, actions=[]
    )
