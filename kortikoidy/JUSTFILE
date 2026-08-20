set dotenv-load := false

rust_dir := "matching_rust"
common_args := "--company cpzp --num-runs 100 --data-dir DATACON_data --out-dir out --specialty-analysis"

# new-repo migration: write everything under new_out/_data and publish under new_out/Finální matchingová analýza
new_out := "new_out/_data"
new_dst := "new_out/Finální matchingová analýza"
new_common := "--num-runs 100 --data-dir DATACON_data --out-dir " + new_out

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

# run the 5 booster (3rd dose) simulations: boosted vs. matched 2-dose and
# matched never-vaccinated pools, 0 PE group only.
# results land under out/<company>/booster_analysis/...
simulate-booster: build
    #!/usr/bin/env bash
    set -e
    for offset in 3 2 1 0 -1; do
        echo "========================================"
        echo "Running booster analysis: year-offset=$offset"
        echo "========================================"
        cargo run --release --manifest-path {{rust_dir}}/Cargo.toml -- {{common_args}} --booster-analysis --year-offset "$offset"
        echo ""
    done
    echo "All booster simulations done."

# === New-repo pipeline: per-company simulations into new_out/_data/ ===

# 5 offsets × 3 modes for CPZP, with --specialty-analysis
simulate-cpzp: build
    #!/usr/bin/env bash
    set -e
    for flag in "" "--inj-analysis" "--every-prescription"; do
        for offset in 3 2 1 0 -1; do
            echo "==== cpzp / offset=$offset ${flag:-"(non-inj)"} ===="
            cargo run --release --manifest-path {{rust_dir}}/Cargo.toml -- \
                --company cpzp {{new_common}} --specialty-analysis \
                --year-offset "$offset" $flag
        done
    done

# 5 offsets × 3 modes for OZP (no specialty data in OZP CSV)
simulate-ozp: build
    #!/usr/bin/env bash
    set -e
    for flag in "" "--inj-analysis" "--every-prescription"; do
        for offset in 3 2 1 0 -1; do
            echo "==== ozp / offset=$offset ${flag:-"(non-inj)"} ===="
            cargo run --release --manifest-path {{rust_dir}}/Cargo.toml -- \
                --company ozp {{new_common}} \
                --year-offset "$offset" $flag
        done
    done

# 5 offsets × 3 modes for both_companies (no specialty: missing in OZP CSV)
simulate-both: build
    #!/usr/bin/env bash
    set -e
    for flag in "" "--inj-analysis" "--every-prescription"; do
        for offset in 3 2 1 0 -1; do
            echo "==== both_companies / offset=$offset ${flag:-"(non-inj)"} ===="
            cargo run --release --manifest-path {{rust_dir}}/Cargo.toml -- \
                --company both_companies {{new_common}} \
                --year-offset "$offset" $flag
        done
    done

# 5 offsets × 3 companies, --immuno-as-corticoid-500pe (only non-inj per existing convention)
simulate-immuno-all: build
    #!/usr/bin/env bash
    set -e
    for company in cpzp ozp both_companies; do
        spec=""
        if [ "$company" = "cpzp" ]; then spec="--specialty-analysis"; fi
        for offset in 3 2 1 0 -1; do
            echo "==== immuno / $company / offset=$offset ===="
            cargo run --release --manifest-path {{rust_dir}}/Cargo.toml -- \
                --company "$company" {{new_common}} $spec \
                --immuno-as-corticoid-500pe --year-offset "$offset"
        done
    done

simulate-all: simulate-cpzp simulate-ozp simulate-both simulate-immuno-all
    @echo "All per-company simulations done."

# generate all forest plots directly into the Czech hierarchy under new_out/Finální matchingová analýza/
plots-all:
    python forest_plot.py --companies cpzp ozp both_companies --out-root {{new_out}} --publish-dir "{{new_dst}}"
    python specialty_forest_plot.py --companies cpzp --out-root {{new_out}} --publish-dir "{{new_dst}}"

# full new-repo pipeline: simulate → plots (plots write directly to Czech hierarchy)
all-new: simulate-all plots-all
    @echo "New-repo pipeline complete. See {{new_dst}}/"

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

# generate forest plots for the booster (3rd dose) analysis
# plots land under out/cpzp/booster_analysis/<mode>/<eb>/forest_plots/
plots-booster:
    python booster_forest_plot.py

# pretty-print all result JSONs (indent=2)
fmt-json:
    find out/cpzp/matching_analysis -name "*.json" -exec python -c "import json,sys;p=sys.argv[1];d=json.load(open(p));json.dump(d,open(p,'w'),indent=2,ensure_ascii=False)" {} \;
    @echo "All JSONs reformatted."

# full pipeline: build → simulate → check → plots
all: simulate check plots
    @echo "Pipeline complete."
