from pathlib import Path
ROOT=Path.cwd(); DOC=ROOT/'docs'; DOC.mkdir(exist_ok=True)
md=["# Co-GM Runbook\n\n","## Pipelines & Outputs\n"]
items=[("Day58","output/trend_rolling.csv, output/age_distribution.csv"),
("Day59","scripts/matchup_sim.py → output/matchup_summary_*.json"),
("Day60","output/league_report.md"),("Day61","pipeline/retry_watchdog.py"),
("Day62","output/mart_star.csv"),("Day63","tests/benchmark_set.csv, logs/day63_fail_samples.csv"),
("Day64","output/trade_value.csv"),("Day65","output/payroll_sim.csv"),
("Day66","output/scenario_alt.csv"),("Day67","output/scouting_report.pdf"),
("Day68","logs/tune_profile.json"),("Day69","docs/runbook.md (this)"),
("Day70","output/weekly_report.md")]
for k,v in items: md.append(f"- **{k}**: {v}\n")
(DOC/'runbook.md').write_text("".join(md), encoding='utf-8'); print("[DAY69] docs/runbook.md written")
