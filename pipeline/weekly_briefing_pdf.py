import matplotlib.pyplot as plt, textwrap
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'
md=(OUT/'weekly_report.md').read_text(encoding='utf-8') if (OUT/'weekly_report.md').exists() else "# Weekly Briefing\n\n(autogen)"
lines=[ln for ln in md.splitlines() if ln.strip().startswith(('- ','##','# '))]
fig=plt.figure(figsize=(8.5,11)); plt.axis('off'); y=0.95
plt.text(0.1,y,"Weekly Operations Briefing", fontsize=18, weight='bold'); y-=0.05
for ln in lines[:60]:
    for w in textwrap.wrap(ln, width=80):
        plt.text(0.08,y,w, fontsize=10); y-=0.025
        if y<0.05: fig.savefig(OUT/'weekly_briefing.pdf'); plt.close(fig); fig=plt.figure(figsize=(8.5,11)); plt.axis('off'); y=0.95
fig.savefig(OUT/'weekly_briefing.pdf'); plt.close(fig)
print("[OK] weekly_briefing.pdf")
