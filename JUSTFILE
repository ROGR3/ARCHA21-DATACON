set dotenv-load := false

rust_dir := "matching_rust"
common_args := "--company cpzp --num-runs 100 --data-dir ../DATACON_data --out-dir ../out --specialty-analysis"

# list available recipes
default:
    @just --list

# build the Rust matching binary (release)
build:
    cargo build --release --manifest-path {{rust_dir}}/Cargo.toml

# run all 15 simulations (5 offsets × 3 analysis modes)
simulate: build
    #!/usr/bin/env bash
    set -e
    for flag in "" "--inj-analysis" "--every-prescription"; do
        for offset in 3 2 1 0 -1; do
            echo "========================================"
            echo "Running: year-offset=$offset ${flag:-"(no extra flag)"}"
            echo "========================================"
            cargo run --release --manifest-path {{rust_dir}}/Cargo.toml -- {{common_args}} --year-offset "$offset" $flag
            echo ""
        done
    done
    echo "All simulations done."

# compare current results against baseline (default 1% tolerance)
check tol="0.01":
    python check_results.py check --tol {{tol}}

# save current results as new baseline
snapshot:
    python check_results.py snapshot

# generate all forest plots (main + per-specialty)
plots:
    python forest_plot.py
    python specialty_forest_plot.py

# pretty-print all result JSONs (indent=2)
fmt-json:
    find out/cpzp/matching_analysis -name "*.json" -exec python -c "import json,sys;p=sys.argv[1];d=json.load(open(p));json.dump(d,open(p,'w'),indent=2,ensure_ascii=False)" {} \;
    @echo "All JSONs reformatted."

# full pipeline: build → simulate → check → plots
all: simulate check plots
    @echo "Pipeline complete."
