use std::collections::HashMap;

use chrono::NaiveDate;
use rand::Rng;
use rayon::prelude::*;

use crate::config::Config;
use crate::types::{AgeCohort, InjMode, PeMap, PeRange, Person};

// ---------------------------------------------------------------------------
// Public result types
// ---------------------------------------------------------------------------

pub type EffectMap = HashMap<AgeCohort, HashMap<NaiveDate, f64>>;
pub type CiMap = HashMap<AgeCohort, HashMap<NaiveDate, (f64, f64)>>;

/// Result of the 3-arm booster matching: boosted persons (effective 3+
/// doses) vs. a matched "2-dose, no booster" pool (Pool B) and a matched
/// "never vaccinated" pool (Pool C). Mirrors `MatchingResult` in
/// `matching.rs`, but carries two comparison pools instead of one.
pub struct BoosterMatchingResult {
    pub boosted_median: EffectMap,
    pub boosted_ci: CiMap,
    pub two_dose_median: EffectMap,
    pub two_dose_ci: CiMap,
    pub novax_median: EffectMap,
    pub novax_ci: CiMap,
    /// boosted effect / two-dose-pool effect (ratio, "VE-style")
    pub effect_vs_two_dose_median: EffectMap,
    pub effect_vs_two_dose_ci: CiMap,
    /// boosted effect / never-vaccinated-pool effect (ratio, "VE-style")
    pub effect_vs_novax_median: EffectMap,
    pub effect_vs_novax_ci: CiMap,
}

// ---------------------------------------------------------------------------
// Single-run output (collected per thread, then merged)
// ---------------------------------------------------------------------------

type FlatEffects = HashMap<AgeCohort, HashMap<NaiveDate, f64>>;

struct RunResult {
    boosted: FlatEffects,
    two_dose: FlatEffects,
    novax: FlatEffects,
    vs_two_dose: FlatEffects,
    vs_novax: FlatEffects,
}

// ---------------------------------------------------------------------------
// Precomputed booster-person data (references into pool candidate slices)
// ---------------------------------------------------------------------------

struct BoosterPersonData<'a> {
    anchor_date: NaiveDate,
    before_pe: f64,
    after_pe: f64,
    ac: AgeCohort,
    agg_date: NaiveDate,
    pool_b_candidates: &'a [usize],
    pool_c_candidates: &'a [usize],
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Runs the 3-arm booster matching analysis. `pool_b_map` and `pool_c_map`
/// must be built over the same `anchor_dates` used to compute
/// `aggregation_days` (typically the full set of distinct booster anchor
/// dates, see `booster_anchor_dates`).
pub fn run_booster_matching_analysis(
    persons: &[Person],
    booster_indices: &[usize],
    pool_b_map: &PeMap,
    pool_c_map: &PeMap,
    config: &Config,
    aggregation_days: i32,
    epoch: NaiveDate,
) -> BoosterMatchingResult {
    let ref_year = config.year_for_age();
    let inj_mode = config.inj_mode;
    let num_runs = config.num_runs;

    let booster_data: Vec<BoosterPersonData> = booster_indices
        .iter()
        .filter_map(|&idx| {
            let p = &persons[idx];
            let anchor_date = p.third_effective_dose_date()?;
            let before_pe = p.pe_before(anchor_date, inj_mode);
            // Booster analysis is restricted to the "0 PE" group: only
            // persons with no prednison-equivalent exposure in the 365 days
            // before their booster anchor date are included.
            if before_pe != 0.0 {
                return None;
            }

            let ac = p.age_cohort(ref_year);
            let gender = p.gender;

            let pool_b_candidates = pool_b_map
                .get(&anchor_date)
                .and_then(|r| r.get(&PeRange::ZeroPe))
                .and_then(|a| a.get(&ac))
                .and_then(|g| g.get(&gender));
            let pool_b_candidates = match pool_b_candidates {
                Some(c) if !c.is_empty() => c,
                _ => return None,
            };

            let pool_c_candidates = pool_c_map
                .get(&anchor_date)
                .and_then(|r| r.get(&PeRange::ZeroPe))
                .and_then(|a| a.get(&ac))
                .and_then(|g| g.get(&gender));
            let pool_c_candidates = match pool_c_candidates {
                Some(c) if !c.is_empty() => c,
                _ => return None,
            };

            let after_pe = p.pe_after(anchor_date, inj_mode);
            let agg_date = aggregate_date(anchor_date, aggregation_days, epoch);

            Some(BoosterPersonData {
                anchor_date,
                before_pe,
                after_pe,
                ac,
                agg_date,
                pool_b_candidates: pool_b_candidates.as_slice(),
                pool_c_candidates: pool_c_candidates.as_slice(),
            })
        })
        .collect();

    eprintln!(
        "    {} boosted persons ready for matching ({} runs × {} threads)",
        booster_data.len(),
        num_runs,
        rayon::current_num_threads(),
    );

    let all_runs: Vec<RunResult> = (0..num_runs)
        .into_par_iter()
        .map(|_| single_run(persons, &booster_data, inj_mode))
        .collect();

    eprintln!("    all {num_runs} runs finished — computing statistics");

    type ValueLists = HashMap<AgeCohort, HashMap<NaiveDate, Vec<f64>>>;
    let mut boosted_lists: ValueLists = HashMap::new();
    let mut two_dose_lists: ValueLists = HashMap::new();
    let mut novax_lists: ValueLists = HashMap::new();
    let mut vs_two_dose_lists: ValueLists = HashMap::new();
    let mut vs_novax_lists: ValueLists = HashMap::new();

    for run in &all_runs {
        merge_into(&mut boosted_lists, &run.boosted);
        merge_into(&mut two_dose_lists, &run.two_dose);
        merge_into(&mut novax_lists, &run.novax);
        merge_into(&mut vs_two_dose_lists, &run.vs_two_dose);
        merge_into(&mut vs_novax_lists, &run.vs_novax);
    }

    BoosterMatchingResult {
        boosted_median: compute_median(&boosted_lists),
        boosted_ci: compute_percentiles(&boosted_lists, 2.5, 97.5),
        two_dose_median: compute_median(&two_dose_lists),
        two_dose_ci: compute_percentiles(&two_dose_lists, 2.5, 97.5),
        novax_median: compute_median(&novax_lists),
        novax_ci: compute_percentiles(&novax_lists, 2.5, 97.5),
        effect_vs_two_dose_median: compute_median(&vs_two_dose_lists),
        effect_vs_two_dose_ci: compute_percentiles(&vs_two_dose_lists, 2.5, 97.5),
        effect_vs_novax_median: compute_median(&vs_novax_lists),
        effect_vs_novax_ci: compute_percentiles(&vs_novax_lists, 2.5, 97.5),
    }
}

// ---------------------------------------------------------------------------
// One matching run
// ---------------------------------------------------------------------------

fn single_run(
    persons: &[Person],
    booster_data: &[BoosterPersonData],
    inj_mode: InjMode,
) -> RunResult {
    let mut rng = rand::thread_rng();

    let mut boosted_before: HashMap<(AgeCohort, NaiveDate), f64> = HashMap::new();
    let mut boosted_after: HashMap<(AgeCohort, NaiveDate), f64> = HashMap::new();
    let mut two_dose_before: HashMap<(AgeCohort, NaiveDate), f64> = HashMap::new();
    let mut two_dose_after: HashMap<(AgeCohort, NaiveDate), f64> = HashMap::new();
    let mut novax_before: HashMap<(AgeCohort, NaiveDate), f64> = HashMap::new();
    let mut novax_after: HashMap<(AgeCohort, NaiveDate), f64> = HashMap::new();
    let mut count: HashMap<(AgeCohort, NaiveDate), f64> = HashMap::new();

    for bd in booster_data {
        let matched_b_idx = bd.pool_b_candidates[rng.gen_range(0..bd.pool_b_candidates.len())];
        let matched_c_idx = bd.pool_c_candidates[rng.gen_range(0..bd.pool_c_candidates.len())];
        let matched_b = &persons[matched_b_idx];
        let matched_c = &persons[matched_c_idx];

        let bb = matched_b.pe_before(bd.anchor_date, inj_mode);
        let ba = matched_b.pe_after(bd.anchor_date, inj_mode);
        let cb = matched_c.pe_before(bd.anchor_date, inj_mode);
        let ca = matched_c.pe_after(bd.anchor_date, inj_mode);

        let key = (bd.ac, bd.agg_date);
        *boosted_before.entry(key).or_default() += bd.before_pe;
        *boosted_after.entry(key).or_default() += bd.after_pe;
        *two_dose_before.entry(key).or_default() += bb;
        *two_dose_after.entry(key).or_default() += ba;
        *novax_before.entry(key).or_default() += cb;
        *novax_after.entry(key).or_default() += ca;
        *count.entry(key).or_default() += 1.0;
    }

    let mut boosted_eff: FlatEffects = HashMap::new();
    let mut two_dose_eff: FlatEffects = HashMap::new();
    let mut novax_eff: FlatEffects = HashMap::new();
    let mut vs_two_dose_eff: FlatEffects = HashMap::new();
    let mut vs_novax_eff: FlatEffects = HashMap::new();

    for (&key, &n) in &count {
        let (ac, dt) = key;
        let bb = *boosted_before.get(&key).unwrap_or(&0.0);
        let ba = *boosted_after.get(&key).unwrap_or(&0.0);
        let tb = *two_dose_before.get(&key).unwrap_or(&0.0);
        let ta = *two_dose_after.get(&key).unwrap_or(&0.0);
        let nb = *novax_before.get(&key).unwrap_or(&0.0);
        let na = *novax_after.get(&key).unwrap_or(&0.0);

        let boosted_diff = (ba - bb) / n;
        let two_dose_diff = (ta - tb) / n;
        let novax_diff = (na - nb) / n;

        boosted_eff.entry(ac).or_default().insert(dt, boosted_diff);
        two_dose_eff.entry(ac).or_default().insert(dt, two_dose_diff);
        novax_eff.entry(ac).or_default().insert(dt, novax_diff);

        if two_dose_diff != 0.0 {
            vs_two_dose_eff
                .entry(ac)
                .or_default()
                .insert(dt, boosted_diff / two_dose_diff);
        }
        if novax_diff != 0.0 {
            vs_novax_eff
                .entry(ac)
                .or_default()
                .insert(dt, boosted_diff / novax_diff);
        }
    }

    RunResult {
        boosted: boosted_eff,
        two_dose: two_dose_eff,
        novax: novax_eff,
        vs_two_dose: vs_two_dose_eff,
        vs_novax: vs_novax_eff,
    }
}

// ---------------------------------------------------------------------------
// Anchor dates helper
// ---------------------------------------------------------------------------

/// Sorted, de-duplicated set of booster anchor dates (i.e. the date each
/// boosted person's effective dose count first reached 3). Used both to
/// build the Pool B / Pool C `PeMap`s and to size the "whole period"
/// aggregation window.
pub fn booster_anchor_dates(persons: &[Person], booster_indices: &[usize]) -> Vec<NaiveDate> {
    let mut dates: Vec<NaiveDate> = booster_indices
        .iter()
        .filter_map(|&idx| persons[idx].third_effective_dose_date())
        .collect();
    dates.sort();
    dates.dedup();
    dates
}

// ---------------------------------------------------------------------------
// Statistics helpers (identical to matching.rs)
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

fn compute_median(data: &HashMap<AgeCohort, HashMap<NaiveDate, Vec<f64>>>) -> EffectMap {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Gender, PersonId, Vaccine};

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    fn person_with_booster(id: u64, third_dose: Option<NaiveDate>) -> Person {
        let mut vaccines = vec![
            Vaccine { date: d(2021, 1, 1), dose_number: 1, effective_dose_number: 1 },
            Vaccine { date: d(2021, 2, 1), dose_number: 2, effective_dose_number: 2 },
        ];
        if let Some(third) = third_dose {
            vaccines.push(Vaccine {
                date: third,
                dose_number: 3,
                effective_dose_number: 3,
            });
        }
        Person {
            id: PersonId::Numeric(id),
            gender: Gender::Male,
            born_year: 1980,
            insurance_start: d(2015, 1, 1),
            insurance_end: d(2050, 12, 31),
            died_at: None,
            vaccines,
            prescriptions: Vec::new(),
        }
    }

    #[test]
    fn booster_anchor_dates_are_sorted_and_deduplicated() {
        let persons = vec![
            person_with_booster(1, Some(d(2021, 12, 1))),
            person_with_booster(2, Some(d(2021, 9, 1))),
            person_with_booster(3, Some(d(2021, 9, 1))), // duplicate date
            person_with_booster(4, None),                // no booster: excluded
        ];
        let idx = vec![0, 1, 2, 3];
        let dates = booster_anchor_dates(&persons, &idx);
        assert_eq!(dates, vec![d(2021, 9, 1), d(2021, 12, 1)]);
    }

    #[test]
    fn booster_anchor_dates_empty_when_no_boosters() {
        let persons = vec![person_with_booster(1, None)];
        let dates = booster_anchor_dates(&persons, &[0]);
        assert!(dates.is_empty());
    }
}
