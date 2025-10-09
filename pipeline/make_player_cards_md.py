#!/usr/bin/env python3
import pandas as pd, os, datetime

SRC = "output/player_cards.csv"
OUT_MD = "output/player_cards_sample.md"
OUT_LOG = f"logs/player_cards_md_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.log"

def emoji_for_league(lg):
    return {"MLB":"⚾", "KBO":"🇰🇷", "MiLB":"🧢"}.get(lg,"❓")

def section(title, icon="📊"):
    return f"\n### {icon} {title}\n"

def render_card(row):
    lg_icon = emoji_for_league(row.get("league",""))
    uid = row.get("player_uid","")
    name = row.get("name_full","Unknown")
    team = row.get("team_id","")
    league = row.get("league","")
    pos = row.get("primary_pos","-")
    bats = row.get("bats","-")
    throws = row.get("throws","-")
    age = round(row.get("age",0),1)
    war = row.get("war","-")
    wrc = row.get("wrc_plus","-")
    fip = row.get("fip","-")
    trend = row.get("trend_form_30d","-")
    std = row.get("standard_scale","-")
    risk = row.get("injury_risk_tier","-")
    ver = row.get("data_version","v55.0")

    md = f"# 🧾 Player Card — {name} ({league} {row.get('season','')})\n\n"
    md += f"**UID:** {uid}  **League:** {league}  **Team:** {team}\n"
    md += f"**Position:** {pos}  **Bats/Throws:** {bats}/{throws}  **Age:** {age}\n"
    md += section("WAR & Performance","🏆")
    md += f"| Metric | Value |\n|:--|:--:|\n| WAR | **{war}** |\n| wRC+ | {wrc} |\n| FIP | {fip} |\n| Trend (30d) | {trend} |\n"
    md += section("Context","🧠")
    md += f"| Factor | Value |\n|:--|:--|\n| Standard Scale | `{std}` |\n| Injury Risk | {risk} |\n| Data Version | {ver} |\n"
    md += section("Summary","🧭")
    md += f"> Performance summary for {name} — generated automatically from Day55 pipeline.\n"
    md += "\n---\n"
    return md

def main():
    if not os.path.exists(SRC):
        print(f"[ERROR] Missing {SRC}")
        return
    df = pd.read_csv(SRC)
    if len(df)==0:
        print("[WARN] No player rows to render")
        return

    os.makedirs("output", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    sample = df.sample(min(10,len(df)), random_state=55)
    cards = [render_card(r) for _,r in sample.iterrows()]
    md = "\n".join(cards)
    with open(OUT_MD,"w",encoding="utf-8") as f: f.write(md)
    with open(OUT_LOG,"w") as log: log.write(f"[OK] Player cards markdown generated at {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n")
    print(f"[OK] {OUT_MD} 생성 완료 ({len(sample)}명)")

if __name__ == "__main__":
    main()

