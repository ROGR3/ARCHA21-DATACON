mod booster_matching;
mod booster_results;
mod cohort_eligibility;
mod config;
mod data_loader;
mod matching;
mod pe_windows;
mod results;
mod types;

use clap::Parser;
use std::time::Instant;

use config::{Cli, Config};
use types::PeGroupName;

fn main() {
    let cli = Cli::parse();
    let config = Config::from_cli(&cli);

    eprintln!("=== Matching Analysis (Rust) ===");
    eprintln!(
        "  company={}, year_offset={}, runs={}, inj={:?}, booster_analysis={}",
        config.company, config.year_offset, config.num_runs, config.inj_mode, config.booster_analysis,
    );

    let t0 = Instant::now();

    // 1. Load persons from CSV
    let loaded = data_loader::load_persons(&config);
    let persons = &loaded.persons;
    eprintln!("  loaded in {:.1}s", t0.elapsed().as_secs_f64());

    if config.booster_analysis {
        run_booster_pipeline(persons, &config, t0);
    } else {
        run_first_dose_pipeline(persons, &config, t0);
    }

    eprintln!("\n=== total: {:.1}s ===", t0.elapsed().as_secs_f64());
}

fn run_first_dose_pipeline(persons: &[types::Person], config: &Config, _t0: Instant) {
    // 2. Split into vax / novax
    let vax_idx = data_loader::vax_people(persons, config);
    let novax_idx = data_loader::novax_people(persons, config);
    eprintln!(
        "  vax={}, novax={} (after insurance filter)",
        vax_idx.len(),
        novax_idx.len(),
    );

    // 3. Compute PeMap from novax people
    let t1 = Instant::now();
    let anchor_dates = config.anchor_dates();
    let pe_map = pe_windows::compute_pe_map(persons, &novax_idx, &anchor_dates, config);
    eprintln!("  pe_map computed in {:.1}s", t1.elapsed().as_secs_f64());

    // 4. Run only 0 PE: this analysis filters both real and virtual
    // vaccination dates by cohort opening + 3 days.
    let aggregation_days = anchor_dates.len() as i32;
    let groups = [PeGroupName::ZeroPe];

    for group_name in &groups {
        let t2 = Instant::now();
        let group_idx = data_loader::filter_group(persons, &vax_idx, *group_name, config);
        eprintln!(
            "\n  group {} — {} persons",
            group_name.label(),
            group_idx.len(),
        );

        if group_idx.is_empty() {
            eprintln!("    skipping (empty group)");
            continue;
        }

        let result = matching::run_matching_analysis(
            persons,
            &group_idx,
            &pe_map,
            config,
            *group_name,
            aggregation_days,
        );

        results::write_results(
            &result,
            persons,
            &group_idx,
            config,
            *group_name,
            aggregation_days,
        );

        eprintln!(
            "    group done in {:.1}s",
            t2.elapsed().as_secs_f64(),
        );
    }
}

/// Booster (3rd dose) analysis: boosted persons (effective ≥3 doses) vs. a
/// matched "2-dose, no booster" pool and a matched "never vaccinated" pool,
/// 0 PE group only. No cohort-opening eligibility gate — boosters aren't
/// subject to the age-cohort first-dose rollout schedule.
fn run_booster_pipeline(persons: &[types::Person], config: &Config, t0: Instant) {
    let t2 = Instant::now();

    let booster_idx = data_loader::booster_people(persons, config);
    let two_dose_idx = data_loader::two_dose_no_booster_people(persons, config);
    let novax_idx = data_loader::novax_people(persons, config);
    eprintln!(
        "  boosted={}, two_dose_no_booster={}, novax={} (after insurance filter)",
        booster_idx.len(),
        two_dose_idx.len(),
        novax_idx.len(),
    );

    if booster_idx.is_empty() {
        eprintln!("  skipping booster analysis (no boosted persons found)");
        return;
    }

    let anchor_dates = booster_matching::booster_anchor_dates(persons, &booster_idx);
    let epoch = *anchor_dates.first().expect("booster_idx non-empty implies dates non-empty");
    let last = *anchor_dates.last().unwrap();
    let aggregation_days = (last - epoch).num_days() as i32 + 1;
    eprintln!(
        "  booster anchor window: {epoch} .. {last} ({} distinct dates, {aggregation_days} day span)",
        anchor_dates.len(),
    );

    let t1 = Instant::now();
    let pool_b_map =
        pe_windows::compute_pe_map_unrestricted(persons, &two_dose_idx, &anchor_dates, config);
    let pool_c_map =
        pe_windows::compute_pe_map_unrestricted(persons, &novax_idx, &anchor_dates, config);
    eprintln!("  pool B/C pe_maps computed in {:.1}s", t1.elapsed().as_secs_f64());

    let result = booster_matching::run_booster_matching_analysis(
        persons,
        &booster_idx,
        &pool_b_map,
        &pool_c_map,
        config,
        aggregation_days,
        epoch,
    );

    booster_results::write_booster_results(&result, persons, &booster_idx, config, aggregation_days);

    eprintln!(
        "  booster analysis done in {:.1}s (total elapsed {:.1}s)",
        t2.elapsed().as_secs_f64(),
        t0.elapsed().as_secs_f64(),
    );
}
