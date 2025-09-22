import os, io, csv, zipfile, sqlite3, sys

# 입력 ZIP/출력 경로
Z = "data/lahman_src.zip"
OUT = "data/lahman.sqlite"

def I(x, d=0):
    try:
        return int(x)
    except:
        return d

if not os.path.exists(Z):
    print("❌ ZIP file not found:", Z)
    sys.exit(1)

with zipfile.ZipFile(Z) as z:
    names = z.namelist()

    def pick(fname):
        c = [n for n in names if n.endswith("/core/" + fname) or n.endswith("/" + fname)]
        if not c:
            raise FileNotFoundError(fname)
        return sorted(c, key=len)[0]

    def load(fname):
        with z.open(pick(fname)) as f:
            return list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8")))

    P  = load("People.csv")
    B  = load("Batting.csv")
    Pi = load("Pitching.csv")
    F  = load("Fielding.csv")
    Tm = load("Teams.csv")

os.makedirs("data", exist_ok=True)
con = sqlite3.connect(OUT); cur = con.cursor()

cur.executescript("""
DROP TABLE IF EXISTS People;
CREATE TABLE People(
  playerID TEXT PRIMARY KEY,
  birthYear INT, birthMonth INT, birthDay INT,
  nameFirst TEXT, nameLast TEXT, bbrefID TEXT, retroID TEXT
);

DROP TABLE IF EXISTS Batting;
CREATE TABLE Batting(
  playerID TEXT, yearID INT, stint INT, teamID TEXT,
  AB INT, H INT, "2B" INT, "3B" INT, HR INT,
  BB INT, IBB INT, HBP INT, SF INT, SH INT, SO INT,
  PRIMARY KEY(playerID,yearID,stint)
);

DROP TABLE IF EXISTS Pitching;
CREATE TABLE Pitching(
  playerID TEXT, yearID INT, stint INT, teamID TEXT,
  W INT, L INT, G INT, GS INT, SV INT,
  IPouts INT, SO INT, BB INT, H INT, HBP INT, ER INT, HR INT,
  PRIMARY KEY(playerID,yearID,stint)
);

DROP TABLE IF EXISTS Fielding;
CREATE TABLE Fielding(
  playerID TEXT, yearID INT, stint INT,
  POS TEXT, G INT, GS INT, InnOuts INT, PO INT, A INT, E INT, DP INT
);

DROP TABLE IF EXISTS Teams;
CREATE TABLE Teams(
  yearID INT, lgID TEXT, teamID TEXT,
  G INT, W INT, L INT,
  AB INT, H INT, "2B" INT, "3B" INT, HR INT,
  BB INT, IBB INT, HBP INT, SF INT,
  SO INT, R INT,
  ER INT, IPouts INT,
  PRIMARY KEY(yearID, teamID)
);
""")

cur.executemany("INSERT INTO People VALUES (?,?,?,?,?,?,?,?)", [
    (r["playerID"], I(r.get("birthYear")), I(r.get("birthMonth")), I(r.get("birthDay")),
     r.get("nameFirst",""), r.get("nameLast",""), r.get("bbrefID",""), r.get("retroID","")) for r in P
])

cur.executemany("INSERT INTO Batting VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
    (r["playerID"], I(r["yearID"]), I(r.get("stint",1)), r.get("teamID",""),
     I(r.get("AB",0)), I(r.get("H",0)), I(r.get("2B",0)), I(r.get("3B",0)), I(r.get("HR",0)),
     I(r.get("BB",0)), I(r.get("IBB",0)), I(r.get("HBP",0)), I(r.get("SF",0)), I(r.get("SH",0)), I(r.get("SO",0)))
    for r in B
])

cur.executemany("INSERT INTO Pitching VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
    (r["playerID"], I(r["yearID"]), I(r.get("stint",1)), r.get("teamID",""),
     I(r.get("W",0)), I(r.get("L",0)), I(r.get("G",0)), I(r.get("GS",0)), I(r.get("SV",0)),
     I(r.get("IPouts",0)), I(r.get("SO",0)), I(r.get("BB",0)), I(r.get("H",0)),
     I(r.get("HBP",0)), I(r.get("ER",0)), I(r.get("HR",0)))
    for r in Pi
])

cur.executemany("INSERT INTO Fielding VALUES (?,?,?,?,?,?,?,?,?,?,?)", [
    (r["playerID"], I(r["yearID"]), I(r.get("stint",1)), r.get("POS",""),
     I(r.get("G",0)), I(r.get("GS",0)), I(r.get("InnOuts",0)), I(r.get("PO",0)),
     I(r.get("A",0)), I(r.get("E",0)), I(r.get("DP",0)))
    for r in F
])

cur.executemany("INSERT INTO Teams VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
    (I(r.get("yearID")), r.get("lgID",""), r.get("teamID",""),
     I(r.get("G",0)), I(r.get("W",0)), I(r.get("L",0)),
     I(r.get("AB",0)), I(r.get("H",0)), I(r.get("2B",0)), I(r.get("3B",0)), I(r.get("HR",0)),
     I(r.get("BB",0)), I(r.get("IBB",0)), I(r.get("HBP",0)), I(r.get("SF",0)),
     I(r.get("SO",0)), I(r.get("R",0)),
     I(r.get("ER",0)), I(r.get("IPouts",0)))
    for r in Tm
])

for sql in [
    "CREATE INDEX IF NOT EXISTS idx_batting_pid_year ON Batting(playerID,yearID)",
    "CREATE INDEX IF NOT EXISTS idx_pitching_pid_year ON Pitching(playerID,yearID)",
    "CREATE INDEX IF NOT EXISTS idx_bat_team_year ON Batting(teamID,yearID)",
    "CREATE INDEX IF NOT EXISTS idx_pit_team_year ON Pitching(teamID,yearID)",
    "CREATE INDEX IF NOT EXISTS idx_fielding_pid_year ON Fielding(playerID,yearID)",
    "CREATE INDEX IF NOT EXISTS idx_teams_year ON Teams(yearID)",
    "CREATE INDEX IF NOT EXISTS idx_teams_year_team ON Teams(yearID,teamID)"
]:
    cur.execute(sql)

con.commit(); con.close()
print("✅ Rebuilt SQLite with teamID in Batting/Pitching + IBB:", OUT)
