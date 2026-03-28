#!/bin/bash
set -e

COMMON_ARGS="--company cpzp --num-runs 100 --data-dir ../DATACON_data --out-dir ../out --specialty-analysis"
YEAR_OFFSETS=(3 2 1 0 -1)

ANALYSIS_FLAGS=(
    ""
    "--inj-analysis"
    "--every-prescription"
)

for flag in "${ANALYSIS_FLAGS[@]}"; do
    for offset in "${YEAR_OFFSETS[@]}"; do
        echo "========================================"
        echo "Running: year-offset=$offset ${flag:-"(no extra flag)"}"
        echo "========================================"
        cargo run --release -- $COMMON_ARGS --year-offset "$offset" $flag
        echo ""
    done
done

echo "All done."
