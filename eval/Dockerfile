# SkeinDB Evaluation Harness — Reproducible Environment
# =====================================================
#
# This Dockerfile builds a minimal container that runs the SkeinDB
# experimental evaluation harness and generates all paper artifacts
# (Tables 3–8, Figures 5–7).
#
# Usage:
#   docker build -t skeindb-eval .
#   docker run skeindb-eval
#
# To extract generated figures:
#   docker run -v $(pwd)/output:/app/output skeindb-eval
#
# The container runs:
#   1. eval/skeindb_experiments_v2.py  — Tables 3–8, Figures 5–6 data
#   2. eval/plot_false_invalidation.py — Figure 7 (false-invalidation curve)

FROM python:3.13-slim

LABEL maintainer="Michél Nguyen <85413447+pinkysworld@users.noreply.github.com>"
LABEL description="SkeinDB paper evaluation harness (IJRCOM v2)"
LABEL org.opencontainers.image.source="https://github.com/pinkysworld/SkeinDB"

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# Install Python dependencies
COPY eval/skeindb_experiments_v2.py eval/
COPY eval/plot_false_invalidation.py eval/

RUN pip install --no-cache-dir blake3 numpy matplotlib

# Create output directories
RUN mkdir -p output/figures

# Default command: run all experiments and generate figures
CMD set -e && \
    echo "========================================" && \
    echo "SkeinDB Evaluation Harness (IJRCOM v2)" && \
    echo "========================================" && \
    echo "" && \
    echo "Step 1/2: Running experimental evaluation..." && \
    python3 eval/skeindb_experiments_v2.py | tee output/experiment_v2_raw_output.txt && \
    echo "" && \
    echo "Step 2/2: Generating false-invalidation figure..." && \
    python3 eval/plot_false_invalidation.py --outdir output/figures && \
    echo "" && \
    echo "========================================" && \
    echo "All experiments complete." && \
    echo "Output written to /app/output/" && \
    echo "  - output/experiment_v2_raw_output.txt" && \
    echo "  - output/figures/fig_false_invalidation.{pdf,png,svg}" && \
    echo "========================================"
