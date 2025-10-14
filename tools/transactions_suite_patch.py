# === Co-GM patch (Day44_v2 integrated) ===
def cmd_day44_v2(args):
    import pandas as pd
    players=pd.read_csv(args.players); teams=pd.read_csv(args.teams)
    deals=[]
    for _,p in players.iterrows():
        if float(p.get("Surplus",0)) < args.min_surplus:
            continue
        for _,t in teams.iterrows():
            if str(t.get("team")) == str(p.get("team")):
                continue
            deals.append({
                "player": p.get("player"),
                "from": p.get("team","NA"),
                "to": t.get("team"),
                "surplus": round(float(p.get("Surplus",0.0)),2)
            })
    pd.DataFrame(deals).to_csv(args.out,index=False)
    print(f"[Day44_v2] -> {args.out} ({len(deals)} deals)")

# main() 안 CLI 등록 부분에 아래 줄 삽입:
# p44v2=sp.add_parser("day44_v2")
# p44v2.add_argument("--players", required=True)
# p44v2.add_argument("--teams", required=True)
# p44v2.add_argument("--min_surplus", type=float, default=1e6)
# p44v2.add_argument("--out", required=True)
# p44v2.set_defaults(func=cmd_day44_v2)
# === Co-GM patch end (Day44_v2 integrated) ===
