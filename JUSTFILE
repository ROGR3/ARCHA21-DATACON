set dotenv-load := false

rust_dir := "matching_rust"
common_args := "--company cpzp --num-runs 100 --data-dir DATACON_data --out-dir out --specialty-analysis"

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

# run the 5 non-INJ simulations with L04 immunosuppressives folded in as 500 PE corticoids
# results land under out/<company>/matching_analysis/immuno_as_corticoid_500pe/non_inj_analysis/...
simulate-immuno: build
    #!/usr/bin/env bash
    set -e
    for offset in 3 2 1 0 -1; do
        echo "========================================"
        echo "Running (immuno-as-corticoid-500pe, non-inj): year-offset=$offset"
        echo "========================================"
        cargo run --release --manifest-path {{rust_dir}}/Cargo.toml -- {{common_args}} --immuno-as-corticoid-500pe --year-offset "$offset"
        echo ""
    done
    echo "All immuno-as-corticoid simulations done."

# compare current results against baseline
check tol="0.01" min_abs="0.01" min_n="120":
    python check_results.py check --tol {{tol}} --min-abs {{min_abs}} --min-n {{min_n}}

# save current results as new baseline
snapshot:
    python check_results.py snapshot

plot-specialty:
    python specialty_forest_plot.py

# generate all forest plots (main + per-specialty)
plots:
    python forest_plot.py
    python specialty_forest_plot.py

# generate forest plots for the immuno-as-corticoid-500pe variant
# plots land under out/cpzp/matching_analysis/immuno_as_corticoid_500pe/<mode>/<eb>/forest_plots/
plots-immuno:
    python forest_plot.py --variant immuno_as_corticoid_500pe
    python specialty_forest_plot.py --variant immuno_as_corticoid_500pe

# pretty-print all result JSONs (indent=2)
fmt-json:
    find out/cpzp/matching_analysis -name "*.json" -exec python -c "import json,sys;p=sys.argv[1];d=json.load(open(p));json.dump(d,open(p,'w'),indent=2,ensure_ascii=False)" {} \;
    @echo "All JSONs reformatted."

# full pipeline: build → simulate → check → plots
all: simulate check plots
    @echo "Pipeline complete."
