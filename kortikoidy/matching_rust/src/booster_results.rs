use std::fs;
use std::path::Path;

use serde::Serialize;

use crate::booster_matching::BoosterMatchingResult;
use crate::config::Config;
use crate::types::{AgeCohort, Person};

#[derive(Serialize)]
struct BoosterSummaryRow {
    #[serde(rename = "věk")]
    age: String,
    #[serde(rename = "počet boosterů")]
    boosted_count: u64,
    #[serde(rename = "boosteři po-před")]
    boosted_effect: Option<f64>,
    #[serde(rename = "boosteři 95% CI")]
    boosted_effect_ci: Option<(f64, f64)>,
    #[serde(rename = "2-davkoví po-před")]
    two_dose_effect: Option<f64>,
    #[serde(rename = "2-davkoví 95% CI")]
    two_dose_effect_ci: Option<(f64, f64)>,
    #[serde(rename = "neočko po-před")]
    novax_effect: Option<f64>,
    #[serde(rename = "neočko 95% CI")]
    novax_effect_ci: Option<(f64, f64)>,
    #[serde(rename = "efekt vs. 2 davky")]
    effect_vs_two_dose: Option<f64>,
    #[serde(rename = "efekt vs. 2 davky 95% CI")]
    effect_vs_two_dose_ci: Option<(f64, f64)>,
    #[serde(rename = "efekt vs. neočko")]
    effect_vs_novax: Option<f64>,
    #[serde(rename = "efekt vs. neočko 95% CI")]
    effect_vs_novax_ci: Option<(f64, f64)>,
}

/// Writes booster analysis results to
/// `out/<company>/booster_analysis/.../whole_period/0_PE/<agg>_days_aggregation/effects_summary.json`
/// — same style as `results::write_results`, but for the 3-arm booster
/// comparison.
pub fn write_booster_results(
    result: &BoosterMatchingResult,
    persons: &[Person],
    booster_indices: &[usize],
    config: &Config,
    aggregation_days: i32,
) {
    let inj_sub = config.inj_subfolder();
    let effect_sub = config.effect_baseline_folder();
    let immuno_sub = config.immuno_subfolder();
    let year_back = format!("{}_years_back_matching_analysis", config.year_offset);

    let out = &config.out_dir;
    let immuno_segment = match immuno_sub {
        Some(s) => format!("{s}/"),
        None => String::new(),
    };
    let inj_segment = if inj_sub.is_empty() {
        String::new()
    } else {
        format!("{inj_sub}/")
    };
    let folder = format!(
        "{out}/{company}/booster_analysis/{immuno_segment}{inj_segment}{effect_sub}/{year_back}/whole_period/0_PE/{agg}_days_aggregation",
        company = config.company,
        agg = aggregation_days,
    );

    fs::create_dir_all(&folder).expect("cannot create output dir");

    let ref_year = config.year_for_age();
    let inj_mode = config.inj_mode;
    let mut boosted_counts: std::collections::HashMap<AgeCohort, u64> = std::collections::HashMap::new();
    for &idx in booster_indices {
        let p = &persons[idx];
        let ac = p.age_cohort(ref_year);
        *boosted_counts.entry(ac).or_default() += 1;
    }
    // Only count persons that actually pass the 0-PE-before-anchor filter used
    // in the matching step (mirrors the same filter in booster_matching.rs).
    let mut boosted_zero_pe_counts: std::collections::HashMap<AgeCohort, u64> =
        std::collections::HashMap::new();
    for &idx in booster_indices {
        let p = &persons[idx];
        if let Some(anchor) = p.third_effective_dose_date() {
            if p.pe_before(anchor, inj_mode) == 0.0 {
                let ac = p.age_cohort(ref_year);
                *boosted_zero_pe_counts.entry(ac).or_default() += 1;
            }
        }
    }
    let rows: Vec<BoosterSummaryRow> = AgeCohort::ALL
        .iter()
        .map(|&ac| {
            let get_first = |map: &crate::booster_matching::EffectMap| -> Option<f64> {
                map.get(&ac).and_then(|dm| dm.values().next()).copied()
            };
            let get_first_ci = |map: &crate::booster_matching::CiMap| -> Option<(f64, f64)> {
                map.get(&ac).and_then(|dm| dm.values().next()).copied()
            };

            BoosterSummaryRow {
                age: ac.label().to_string(),
                boosted_count: *boosted_zero_pe_counts.get(&ac).unwrap_or(&0),
                boosted_effect: get_first(&result.boosted_median),
                boosted_effect_ci: get_first_ci(&result.boosted_ci),
                two_dose_effect: get_first(&result.two_dose_median),
                two_dose_effect_ci: get_first_ci(&result.two_dose_ci),
                novax_effect: get_first(&result.novax_median),
                novax_effect_ci: get_first_ci(&result.novax_ci),
                effect_vs_two_dose: get_first(&result.effect_vs_two_dose_median),
                effect_vs_two_dose_ci: get_first_ci(&result.effect_vs_two_dose_ci),
                effect_vs_novax: get_first(&result.effect_vs_novax_median),
                effect_vs_novax_ci: get_first_ci(&result.effect_vs_novax_ci),
            }
        })
        .collect();

    let json_path = Path::new(&folder).join("../effects_summary.json");
    let parent = json_path.parent().unwrap();
    fs::create_dir_all(parent).ok();
    let json = serde_json::to_string(&rows).expect("JSON serialization failed");
    fs::write(&json_path, &json).expect("cannot write JSON");

    eprintln!("  wrote {}", json_path.display());
    eprintln!("  (total booster persons found, before 0-PE filter: {boosted_counts:?})");
}
