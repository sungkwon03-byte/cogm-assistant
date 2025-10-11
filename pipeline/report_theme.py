import matplotlib as mpl
from matplotlib import pyplot as plt

def _safe_font_family():
    preferred = ["Noto Sans CJK KR", "Noto Sans", "DejaVu Sans", "Arial", "Liberation Sans"]
    installed = {f.name for f in mpl.font_manager.fontManager.ttflist}
    for name in preferred:
        if name in installed:
            return [name]
    return ["DejaVu Sans"]

def apply_theme(theme: str = "mono", accent: str = "red"):
    mpl.rcdefaults()
    mpl.rcParams["font.family"] = _safe_font_family()
    mpl.rcParams["axes.titlesize"] = 12
    mpl.rcParams["axes.labelsize"] = 10
    mpl.rcParams["xtick.labelsize"] = 9
    mpl.rcParams["ytick.labelsize"] = 9
    mpl.rcParams["figure.dpi"] = 120
    mpl.rcParams["savefig.dpi"] = 160
    mpl.rcParams["axes.linewidth"] = 0.8
    mpl.rcParams["grid.linewidth"] = 0.4

    if theme == "mono":
        mpl.rcParams["figure.facecolor"] = "white"
        mpl.rcParams["axes.facecolor"] = "white"
        mpl.rcParams["text.color"]  = "black"
        mpl.rcParams["axes.edgecolor"] = "black"
        mpl.rcParams["axes.labelcolor"] = "black"
        mpl.rcParams["xtick.color"] = "black"
        mpl.rcParams["ytick.color"] = "black"
        mpl.rcParams["grid.color"] = "#DDDDDD"
    elif theme == "light":
        mpl.rcParams["figure.facecolor"] = "white"
        mpl.rcParams["axes.facecolor"] = "white"
        mpl.rcParams["text.color"]  = "#111111"
        mpl.rcParams["axes.edgecolor"] = "#222222"
        mpl.rcParams["grid.color"] = "#E6E6E6"
    else:
        mpl.rcParams["figure.facecolor"] = "#0D0D0D"
        mpl.rcParams["axes.facecolor"] = "#0D0D0D"
        mpl.rcParams["text.color"]  = "#F2F2F2"
        mpl.rcParams["axes.edgecolor"] = "#D0D0D0"
        mpl.rcParams["axes.labelcolor"] = "#F2F2F2"
        mpl.rcParams["xtick.color"] = "#F2F2F2"
        mpl.rcParams["ytick.color"] = "#F2F2F2"
        mpl.rcParams["grid.color"] = "#2A2A2A"

    mpl.rcParams["axes.prop_cycle"] = plt.cycler(
        color=["#C62828" if accent=="red" else "#111111", "#616161", "#9E9E9E", "#BDBDBD"]
    )

def caption(ax, text: str):
    ax.text(0.0, 1.02, text, transform=ax.transAxes,
            ha="left", va="bottom", fontsize=9,
            color=ax.spines["left"].get_edgecolor())
