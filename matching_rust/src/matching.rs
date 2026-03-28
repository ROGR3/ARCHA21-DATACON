use std::collections::HashMap;

use chrono::NaiveDate;
use rand::Rng;
use rayon::prelude::*;

use crate::config::Config;
use crate::types::{AgeCohort, InjMode, PeGroupName, PeMap, PeRange, Person};

// ---------------------------------------------------------------------------
// Public result types
// ---------------------------------------------------------------------------

pub type EffectMap = HashMap<AgeCohort, HashMap<NaiveDate, f64>>;
pub type CiMap = HashMap<AgeCohort, HashMap<NaiveDate, (f64, f64)>>;

pub struct MatchingResult {
    pub median: EffectMap,
    pub iqr: CiMap,
    pub ci: CiMap,
    pub vax_median: EffectMap,
    pub novax_median: EffectMap,
    pub vax_ci: CiMap,
    pub novax_ci: CiMap,
}

// ---------------------------------------------------------------------------
// Single-run output (collected per thread, then merged)
// ---------------------------------------------------------------------------

struct RunResult {
    effects: HashMap<AgeCohort, HashMap<NaiveDate, f64>>,
    vax_effects: HashMap<AgeCohort, HashMap<NaiveDate, f64>>,
    novax_effects: HashMap<AgeCohort, HashMap<NaiveDate, f64>>,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub fn run_matching_analysis(
    persons: &[Person],
    group_indices: &[usize],
    pe_map: &PeMap,
    config: &Config,
    group_name: PeGroupName,
    aggregation_days: i32,
) -> MatchingResult {
    let ref_year = config.year_for_age();
    let inj_mode = config.inj_mode;
    let unified = config.unified_effect_baseline;
    let epoch = config.start_date();
    let num_runs = config.num_runs;

    // Precompute per-vax-person invariants (avoids recalculating 100 times).
    let vax_data: Vec<VaxPersonData> = group_indices
        .iter()
        .filter_map(|&idx| {
            let p = &persons[idx];
            let vax_date = p.first_vax_date()?;
            let before_pe = p.pe_before(vax_date, inj_mode);
            if before_pe > 5000.0 {
                return None;
            }

            let pe_range = match group_name {
                PeGroupName::NeverPrescribed => PeRange::ZeroNoPre,
                PeGroupName::ZeroPeSuspectible => PeRange::ZeroPeSuspectible,
                _ => PeRange::from_pe(before_pe),
            };

            let ac = p.age_cohort(ref_year);
            let gender = p.gender;

            let candidates = pe_map
                .get(&vax_date)
                .and_then(|r| r.get(&pe_range))
                .and_then(|a| a.get(&ac))
                .and_then(|g| g.get(&gender));

            let candidates = match candidates {
                Some(c) if !c.is_empty() => c,
                _ => return None,
            };

            let after_pe = p.pe_after(vax_date, inj_mode);
            let agg_date = aggregate_date(vax_date, aggregation_days, epoch);

            Some(VaxPersonData {
                vax_date,
                before_pe,
                after_pe,
                ac,
                agg_date,
                candidate_indices: candidates.as_slice(),
            })
        })
        .collect();

    eprintln!(
        "    {} vax persons ready for matching ({} runs × {} threads)",
        vax_data.len(),
        num_runs,
        rayon::current_num_threads(),
    );

    // Run all iterations in parallel via Rayon
    let all_runs: Vec<RunResult> = (0..num_runs)
        .into_par_iter()
        .map(|_| {
            single_run(
                persons, &vax_data, inj_mode, group_name, unified,
            )
        })
        .collect();

    eprintln!("    all {num_runs} runs finished — computing statistics");

    // Merge into per-(cohort, date) value lists
    let mut effects_lists: HashMap<AgeCohort, HashMap<NaiveDate, Vec<f64>>> = HashMap::new();
    let mut vax_lists: HashMap<AgeCohort, HashMap<NaiveDate, Vec<f64>>> = HashMap::new();
    let mut novax_lists: HashMap<AgeCohort, HashMap<NaiveDate, Vec<f64>>> = HashMap::new();

    for run in &all_runs {
        merge_into(&mut effects_lists, &run.effects);
        merge_into(&mut vax_lists, &run.vax_effects);
        merge_into(&mut novax_lists, &run.novax_effects);
    }

    MatchingResult {
        median: compute_median(&effects_lists),
        iqr: compute_percentiles(&effects_lists, 25.0, 75.0),
        ci: compute_percentiles(&effects_lists, 2.5, 97.5),
        vax_median: compute_median(&vax_lists),
        novax_median: compute_median(&novax_lists),
        vax_ci: compute_percentiles(&vax_lists, 2.5, 97.5),
        novax_ci: compute_percentiles(&novax_lists, 2.5, 97.5),
    }
}

// ---------------------------------------------------------------------------
// Precomputed vax-person data (references into pe_map candidate slices)
// ---------------------------------------------------------------------------

struct VaxPersonData<'a> {
    vax_date: NaiveDate,
    before_pe: f64,
    after_pe: f64,
    ac: AgeCohort,
    agg_date: NaiveDate,
    candidate_indices: &'a [usize],
}

// ---------------------------------------------------------------------------
// One matching run
// ---------------------------------------------------------------------------

fn single_run(
    persons: &[Person],
    vax_data: &[VaxPersonData],
    inj_mode: InjMode,
    group_name: PeGroupName,
    unified: bool,
) -> RunResult {
    let mut rng = rand::thread_rng();

    let mut vax_before: HashMap<(AgeCohort, NaiveDate), f64> = HashMap::new();
    let mut vax_after: HashMap<(AgeCohort, NaiveDate), f64> = HashMap::new();
    let mut novax_before: HashMap<(AgeCohort, NaiveDate), f64> = HashMap::new();
    let mut novax_after: HashMap<(AgeCohort, NaiveDate), f64> = HashMap::new();
    let mut count: HashMap<(AgeCohort, NaiveDate), f64> = HashMap::new();

    for vd in vax_data {
        let matched_idx = vd.candidate_indices[rng.gen_range(0..vd.candidate_indices.len())];
        let matched = &persons[matched_idx];

        let nb = matched.pe_before(vd.vax_date, inj_mode);
        let na = matched.pe_after(vd.vax_date, inj_mode);

        let key = (vd.ac, vd.agg_date);
        *vax_before.entry(key).or_default() += vd.before_pe;
        *vax_after.entry(key).or_default() += vd.after_pe;
        *novax_before.entry(key).or_default() += nb;
        *novax_after.entry(key).or_default() += na;
        *count.entry(key).or_default() += 1.0;
    }

    // Compute effects
    let mut effects: HashMap<AgeCohort, HashMap<NaiveDate, f64>> = HashMap::new();
    let mut vax_eff: HashMap<AgeCohort, HashMap<NaiveDate, f64>> = HashMap::new();
    let mut novax_eff: HashMap<AgeCohort, HashMap<NaiveDate, f64>> = HashMap::new();

    let all_keys: Vec<_> = vax_before.keys().chain(novax_before.keys()).copied().collect();
    for key in all_keys {
        let (ac, dt) = key;
        let vb = *vax_before.get(&key).unwrap_or(&0.0);
        let va = *vax_after.get(&key).unwrap_or(&0.0);
        let nb = *novax_before.get(&key).unwrap_or(&0.0);
        let na = *novax_after.get(&key).unwrap_or(&0.0);
        let n = *count.get(&key).unwrap_or(&1.0);

        if !unified && !group_name.is_zero_pe_group() {
            if vb != 0.0 && nb != 0.0 {
                let ve = va / vb;
                let ne = na / nb;
                vax_eff.entry(ac).or_default().insert(dt, ve);
                novax_eff.entry(ac).or_default().insert(dt, ne);
                effects.entry(ac).or_default().insert(dt, ve - ne);
            }
        } else {
            let ve = (va - vb) / n;
            let ne = (na - nb) / n;
            vax_eff.entry(ac).or_default().insert(dt, ve);
            novax_eff.entry(ac).or_default().insert(dt, ne);
            if ne != 0.0 {
                effects.entry(ac).or_default().insert(dt, ve / ne);
            }
        }
    }

    RunResult {
        effects,
        vax_effects: vax_eff,
        novax_effects: novax_eff,
    }
}

// ---------------------------------------------------------------------------
// Statistics helpers
// ---------------------------------------------------------------------------

fn merge_into(
    dst: &mut HashMap<AgeCohort, HashMap<NaiveDate, Vec<f64>>>,
    src: &HashMap<AgeCohort, HashMap<NaiveDate, f64>>,
) {
    for (&ac, date_map) in src {
        for (&dt, &val) in date_map {
            dst.entry(ac).or_default().entry(dt).or_default().push(val);
        }
    }
}

fn compute_median(
    data: &HashMap<AgeCohort, HashMap<NaiveDate, Vec<f64>>>,
) -> EffectMap {
    let mut out = HashMap::new();
    for (&ac, date_map) in data {
        for (&dt, vals) in date_map {
            let median = percentile(vals, 50.0);
            out.entry(ac).or_insert_with(HashMap::new).insert(dt, median);
        }
    }
    out
}

fn compute_percentiles(
    data: &HashMap<AgeCohort, HashMap<NaiveDate, Vec<f64>>>,
    lo: f64,
    hi: f64,
) -> CiMap {
    let mut out = HashMap::new();
    for (&ac, date_map) in data {
        for (&dt, vals) in date_map {
            let p_lo = percentile(vals, lo);
            let p_hi = percentile(vals, hi);
            out.entry(ac)
                .or_insert_with(HashMap::new)
                .insert(dt, (p_lo, p_hi));
        }
    }
    out
}

fn percentile(values: &[f64], pct: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted: Vec<f64> = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = sorted.len();
    let idx = (pct / 100.0) * (n as f64 - 1.0);
    let lo = idx.floor() as usize;
    let hi = idx.ceil() as usize;
    if lo == hi || hi >= n {
        sorted[lo.min(n - 1)]
    } else {
        let frac = idx - lo as f64;
        sorted[lo] * (1.0 - frac) + sorted[hi] * frac
    }
}

fn aggregate_date(date: NaiveDate, window_days: i32, epoch: NaiveDate) -> NaiveDate {
    if window_days <= 1 {
        return date;
    }
    let days_since = (date - epoch).num_days() as i32;
    let start = (days_since / window_days) * window_days;
    epoch + chrono::Duration::days(start as i64)
}
