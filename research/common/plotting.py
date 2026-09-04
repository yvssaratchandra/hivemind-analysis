"""Figure helpers for the Hivemind network-security experiments.

Deliberately thin: matplotlib with a consistent look, saved as PNG next to the CSV
that produced it. Every figure is reproducible from its CSV, so the plots are a
view of the data rather than the record of it.

matplotlib is an optional dependency here (it is not in Hivemind's requirements);
experiments must still run and write CSVs when it is absent.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple

__all__ = ["available", "line_plot", "ATTACKER_COLOR", "HONEST_COLOR"]

# One consistent semantic: red = attacker's advantage, blue = honest/baseline.
ATTACKER_COLOR = "#c1121f"
HONEST_COLOR = "#0353a4"
REFERENCE_COLOR = "#6c757d"


def available() -> bool:
    """True if matplotlib can be imported, so callers can skip plotting gracefully."""
    try:
        import matplotlib  # noqa: F401
    except ImportError:
        return False
    return True


def line_plot(
    series: Dict[str, Tuple[Sequence[float], Sequence[float]]],
    *,
    path: Path | str,
    title: str,
    xlabel: str,
    ylabel: str,
    errorbars: Optional[Dict[str, Sequence[float]]] = None,
    reference_line: Optional[float] = None,
    reference_label: Optional[str] = None,
    ylim: Optional[Tuple[float, float]] = None,
    logx: bool = False,
) -> Optional[Path]:
    """Plot ``{label: (x, y)}`` to ``path``. Returns None if matplotlib is missing.

    ``reference_line`` draws a horizontal marker -- used throughout to show the
    'no attack advantage' baseline (e.g. f_l == f_g), which is what the attacker's
    curve should be read against.
    """
    if not available():
        return None

    import matplotlib

    matplotlib.use("Agg")  # headless: these run in scripts, not notebooks
    import matplotlib.pyplot as plt

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    figure, axes = plt.subplots(figsize=(7, 4.5), dpi=140)
    palette = [ATTACKER_COLOR, HONEST_COLOR, "#588157", "#e07a5f", "#7209b7"]

    for index, (label, (xs, ys)) in enumerate(series.items()):
        color = palette[index % len(palette)]
        yerr = (errorbars or {}).get(label)
        if yerr is not None:
            axes.errorbar(xs, ys, yerr=yerr, label=label, color=color, marker="o", capsize=3, linewidth=1.8)
        else:
            axes.plot(xs, ys, label=label, color=color, marker="o", linewidth=1.8)

    if reference_line is not None:
        axes.axhline(
            reference_line,
            color=REFERENCE_COLOR,
            linestyle="--",
            linewidth=1.2,
            label=reference_label or "reference",
        )

    axes.set_title(title)
    axes.set_xlabel(xlabel)
    axes.set_ylabel(ylabel)
    if ylim is not None:
        axes.set_ylim(*ylim)
    if logx:
        axes.set_xscale("log")
    axes.grid(alpha=0.25, linestyle=":")
    axes.legend(frameon=False, fontsize=9)
    figure.tight_layout()
    figure.savefig(path)
    plt.close(figure)
    return path
