import os, glob, shutil, zipfile, csv, io, sqlite3, sys
# 1) ZIP 자동 탐색
cands = glob.glob("/mnt/data/lahman*csv*.zip") + glob.glob("/workspaces/**/lahman*csv*.zip", recursive=True)
if not cands:
    print("ERR: CSV ZIP not found (pattern: lahman*csv*.zip)"); sys.exit(2)
src = sorted(cands, key=len)[0]
os.makedirs("data", exist_ok=True)
dst = "data/lahman_src.zip"
shutil.copyfile(src, dst)
print("[ZIP]", src, "->", dst)

# 2) CSV -> SQLite (핵심 테이블)
def I(x):
    try: return int(x)
    except: return 0
with zipfile.ZipFile(dst) as z:
    names = z.namelist()
    def pick(fname):
        m=[n for n in names if n.endswith("/core/"+fname) or n.endswith("/"+fname)]
        if not m: raise FileNotFoundError(fname)
        return sorted(m,key=len)[0]
    def load(fname):
        with z.open(pick(fname)) as f:
            return list(csv.DictReader(io.TextIOWrapper(f,encoding="utf-8")))
    P = load("People.csv"); B = load("Batting.csv"); Pi = load("Pitching.csv"); F = load("Fielding.csv")

out = "data/lahman.sqlite"
con = sqlite3.connect(out); cur = con.cursor()
cur.executescript("""
DROP TABLE IF EXISTS People;
CREATE TABLE People(playerID TEXT PRIMARY KEY,nameFirst TEXT,nameLast TEXT,bbrefID TEXT,retroID TEXT);
DROP TABLE IF EXISTS Batting;
CREATE TABLE Batting(playerID TEXT,yearID INT,stint INT,AB INT,H INT,"2B" INT,"3B" INT,HR INT,BB INT,HBP INT,SF INT,SH INT,SO INT);
DROP TABLE IF EXISTS Pitching;
CREATE TABLE Pitching(playerID TEXT,yearID INT,stint INT,W INT,L INT,G INT,GS INT,SV INT,IPouts INT,SO INT,BB INT,H INT,HBP INT,ER INT);
DROP TABLE IF EXISTS Fielding;
CREATE TABLE Fielding(playerID TEXT,yearID INT,stint INT,POS TEXT,G INT,GS INT,InnOuts INT,PO INT,A INT,E INT,DP INT);
""")
cur.executemany("INSERT INTO People VALUES (?,?,?,?,?)",
    [(r["playerID"], r.get("nameFirst",""), r.get("nameLast",""), r.get("bbrefID",""), r.get("retroID","")) for r in P])
cur.executemany("INSERT INTO Batting VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
    [(r["playerID"],I(r["yearID"]),I(r.get("stint",1)),I(r.get("AB",0)),I(r.get("H",0)),I(r.get("2B",0)),I(r.get("3B",0)),
      I(r.get("HR",0)),I(r.get("BB",0)),I(r.get("HBP",0)),I(r.get("SF",0)),I(r.get("SH",0)),I(r.get("SO",0))) for r in B])
cur.executemany("INSERT INTO Pitching VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
    [(r["playerID"],I(r["yearID"]),I(r.get("stint",1)),I(r.get("W",0)),I(r.get("L",0)),I(r.get("G",0)),I(r.get("GS",0)),I(r.get("SV",0)),
      I(r.get("IPouts",0)),I(r.get("SO",0)),I(r.get("BB",0)),I(r.get("H",0)),I(r.get("HBP",0)),I(r.get("ER",0))) for r in Pi])
cur.executemany("INSERT INTO Fielding VALUES (?,?,?,?,?,?,?,?,?,?,?)",
    [(r["playerID"],I(r["yearID"]),I(r.get("stint",1)),r.get("POS",""),I(r.get("G",0)),I(r.get("GS",0)),I(r.get("InnOuts",0)),
      I(r.get("PO",0)),I(r.get("A",0)),I(r.get("E",0)),I(r.get("DP",0))) for r in F])
con.commit()
for t in ("People","Batting","Pitching","Fielding"):
    cur.execute(f"select count(*) from {t}"); print(f"[OK] {t} rows:", cur.fetchone()[0])
con.close(); print("[BUILT]", out)
