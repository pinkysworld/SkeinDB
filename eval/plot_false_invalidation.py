#!/usr/bin/env python3
"""Generate false-invalidation figures for SkeinDB paper (Figure 7).

Produces fig_false_invalidation.{pdf,png,svg} showing the modeled
false-invalidation rate as a function of predicate selectivity s under
table-level dependency tracking (uniform-write model).

Usage:
  python3 plot_false_invalidation.py --outdir figures

Dependencies:
  pip install numpy matplotlib

This script is not required to reproduce the core artifact tables (Tables 3–8),
but it regenerates the modeled false-invalidation curve (Figure 7) used in the
paper's Section 4.5.
"""

from __future__ import annotations

import argparse
import os
import sys

import numpy as np

try:
    import matplotlib
    matplotlib.use("Agg")  # non-interactive backend for CI/Docker
    import matplotlib.pyplot as plt
except ImportError:
    print("ERROR: matplotlib is required. Install via: pip install matplotlib", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Generate false-invalidation figure for SkeinDB paper (Figure 7)."
    )
    ap.add_argument(
        "--outdir",
        default="figures",
        help="Output directory for generated figures (default: figures)",
    )
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    # Selectivity range: from 0.01% to 100%
    x = np.logspace(-4, 0, 200)  # selectivity s

    # Under table-level deps, every write invalidates the query.
    # True invalidation probability = s (the write actually affects the result).
    # False invalidation rate = 1 - s.
    y_false = 1 - x

    # Ideal (perfectly precise) dependency tracking: zero false invalidations.
    y_ideal = np.zeros_like(x)

    # Key-range dependency tracking (estimated improvement): false rate ≈ max(0, 1 - s/s_range)
    # where s_range is the fraction of the key space covered by the dependency set.
    # For a well-targeted predicate, s_range ≈ s, so false rate approaches 0.
    # For a moderately precise estimate (10× better than table-level):
    y_keyrange = np.clip(1 - x * 10, 0, 1)

    plt.figure(figsize=(6, 3.6))
    plt.semilogx(x, y_false, "b-", linewidth=1.5, label="Table-level deps (current, modeled)")
    plt.semilogx(x, y_keyrange, "g--", linewidth=1.2, label="Key-range deps (estimated, planned)")
    plt.semilogx(x, y_ideal, "k:", linewidth=1.0, label="Perfectly precise deps (ideal)")

    # Mark specific data points from Table 7
    table7_s = np.array([0.001, 0.01, 0.10, 0.50])
    table7_false = 1 - table7_s
    plt.plot(table7_s, table7_false, "ro", markersize=5, label="Table 7 data points")

    plt.ylim(-0.02, 1.05)
    plt.xlim(1e-4, 1.0)
    plt.xlabel("Predicate selectivity $s$ (fraction of rows)", fontsize=10)
    plt.ylabel("False invalidation rate", fontsize=10)
    plt.title("False Invalidations Under Coarse Dependencies", fontsize=11)
    plt.legend(loc="lower left", fontsize=8, frameon=True, fancybox=True, framealpha=0.9)
    plt.grid(True, alpha=0.3, which="both")
    plt.tight_layout()

    for ext, kwargs in [("pdf", {}), ("png", {"dpi": 300}), ("svg", {})]:
        outpath = os.path.join(args.outdir, f"fig_false_invalidation.{ext}")
        plt.savefig(outpath, **kwargs)
        print(f"  Saved: {outpath}")

    plt.close()
    print("Done.")


if __name__ == "__main__":
    main()
