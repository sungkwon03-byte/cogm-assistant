#!/usr/bin/env python3
import os, glob, pandas as pd

SEARCH_DIRS=["mart","data","external","inputs","downloads","."]
HINTS = ("age","seasonage","dob","birth")

def list_csvs():
    out=[]
    for root in SEARCH_DIRS:
        if not os.path.isdir(root): continue
        for p in glob.glob(os.path.join(root,"**","*.csv"), recursive=True):
            P=p.lower()
            if any(x in P for x in ("/logs/","/output/","/venv/","/env/","minor","milb")): 
                continue
            try:
                if os.path.getsize(p)>0:
                    out.append(p)
            except Exception:
                pass
    return sorted(set(out))

def main():
    files=list_csvs()
    rows=[]
    for f in files:
        try:
            head=pd.read_csv(f, nrows=0, low_memory=False)
        except Exception:
            continue
        cols=head.columns.tolist()
        hits=[c for c in cols if any(h in c.lower() for h in HINTS)]
        if hits:
            rows.append((f, len(cols), hits[:20]))
    if not rows:
        print("[-] age/dob/birth 관련 컬럼 있는 파일을 찾지 못함.")
        return
    print(f"[INFO] 후보 파일: {len(rows)}개")
    for f,n,hits in rows:
        print(f" - {f} | cols={n} | hits={hits}")
if __name__=="__main__":
    main()
