from app.lib.name_resolver import resolve_names
import os, duckdb, functools
def ok(p):
    try: return os.path.getsize(p)>0
    except: return False
CARDS="output/player_cards_all.parquet"
STAT ="output/statcast_ultra_full_clean.parquet"
IDM  ="output/id_map.csv"
_con=None
def con():
    global _con
    if _con is None:
        os.makedirs("data/duckdb", exist_ok=True)
        _con=duckdb.connect("data/duckdb/main.duckdb")
    return _con
def ensure_views():
    c=con()
    if ok(CARDS): c.execute("CREATE OR REPLACE VIEW cards AS SELECT * FROM read_parquet(?, union_by_name=true)", [CARDS])
    if ok(STAT):  c.execute("CREATE OR REPLACE VIEW statcast AS SELECT * FROM read_parquet(?, union_by_name=true)", [STAT])
    if ok(IDM):   c.execute("CREATE OR REPLACE VIEW id_map AS SELECT * FROM read_csv_auto(?, header=true)", [IDM])
def table_exists(name:str)->bool:
    try: con().execute(f"SELECT 1 FROM {name} LIMIT 1").fetchone(); return True
    except: return False
def cols(name:str):
    try: return [r[1] for r in con().execute(f"PRAGMA table_info('{name}')").fetchall()]
    except: return []
def df(sql:str):
    return con().execute(sql).df()
