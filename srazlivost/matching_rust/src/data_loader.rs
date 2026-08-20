use std::collections::HashMap;
use std::path::Path;

use chrono::NaiveDate;

use crate::cohort_eligibility;
use crate::config::Config;
use crate::types::{Gender, PeGroupName, Person, PersonId, Prescription, SpecialtyGroup, Vaccine};

// ---------------------------------------------------------------------------
// CSV row – one row from the flat CSV file
// ---------------------------------------------------------------------------

/// Columns shared between CPZP and OZP (all optional because CSV has NAs).
struct RawRow {
    gender: String,
    birth_year: Option<i32>,
    insurance_start: Option<NaiveDate>,
    insurance_end: Option<NaiveDate>,
    death_date: Option<NaiveDate>,
    event_type: String,
    event_date: Option<NaiveDate>,
    atc_skupina: Option<String>,
    lekova_forma_zkr: Option<String>,
    specializace: Option<String>,
    prednison_equiv: Option<f64>,
    pocet_baleni: Option<f64>,
    pocet_v_baleni: Option<f64>,
    sila: Option<String>,
    /// CPZP: free-text vaccine name (e.g. "COVID-19 - OČKOVÁNÍ - JOHNSON & JOHNSON").
    kod_udalosti: Option<String>,
    /// OZP: numeric VZP performance code (e.g. "99933"). CPZP: same code, unused here.
    detail_udalosti: Option<String>,
}

/// VZP performance codes that identify a Johnson & Johnson (Janssen) injection.
/// Mirrors `OZP_VACCINE_NAZEV` codes 99933 / 99939 in `reshape_common.py`.
const JANSSEN_OZP_CODES: [&str; 2] = ["99933", "99939"];

/// True if this vaccination row is a Janssen (Johnson & Johnson) injection —
/// a single-shot vaccine that counts as 2 effective doses.
fn is_janssen_row(row: &RawRow, is_cpzp: bool) -> bool {
    if is_cpzp {
        row.kod_udalosti
            .as_deref()
            .is_some_and(|s| s.to_uppercase().contains("JOHNSON"))
    } else {
        row.detail_udalosti
            .as_deref()
            .is_some_and(|s| JANSSEN_OZP_CODES.contains(&s))
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub struct LoadedData {
    pub persons: Vec<Person>,
    pub person_index: HashMap<PersonId, usize>,
}

pub fn load_persons(config: &Config) -> LoadedData {
    let mut persons = match config.company.as_str() {
        "both_companies" => {
            let mut cpzp = load_csv(&format!("{}/CPZP_preskladane.csv", config.data_dir), true, config);
            let ozp = load_csv(&format!("{}/OZP_preskladane.csv", config.data_dir), false, config);
            cpzp.extend(ozp);
            cpzp
        }
        "cpzp" => load_csv(&format!("{}/CPZP_preskladane.csv", config.data_dir), true, config),
        "ozp" => load_csv(&format!("{}/OZP_preskladane.csv", config.data_dir), false, config),
        other => panic!("Unknown company: {other}"),
    };

    // Sort prescriptions by date for each person (used by binary-search in pe_windows)
    for p in &mut persons {
        p.prescriptions.sort_by_key(|pr| pr.date);
    }

    eprintln!("Loaded {} persons", persons.len());

    let person_index: HashMap<PersonId, usize> = persons
        .iter()
        .enumerate()
        .map(|(i, p)| (p.id.clone(), i))
        .collect();

    LoadedData {
        persons,
        person_index,
    }
}

// ---------------------------------------------------------------------------
// Group filters (mirrors DataLoader in Python)
// ---------------------------------------------------------------------------

pub fn vax_people(persons: &[Person], config: &Config) -> Vec<usize> {
    persons
        .iter()
        .enumerate()
        .filter(|(_, p)| {
            !p.vaccines.is_empty()
                && p.died_at.is_none()
                && p.insurance_start < config.insurance_start
                && p.insurance_end > config.insurance_end
                && config.max_doses.is_none_or(|n| p.vaccines.len() <= n)
        })
        .map(|(i, _)| i)
        .collect()
}

pub fn novax_people(persons: &[Person], config: &Config) -> Vec<usize> {
    persons
        .iter()
        .enumerate()
        .filter(|(_, p)| {
            p.vaccines.is_empty()
                && p.died_at.is_none()
                && p.insurance_start < config.insurance_start
                && p.insurance_end > config.insurance_end
        })
        .map(|(i, _)| i)
        .collect()
}

/// Boosted persons: effective dose count reached ≥3 at some point (Janssen's
/// first-shot-counts-as-2 rule already baked into `effective_dose_number`).
pub fn booster_people(persons: &[Person], config: &Config) -> Vec<usize> {
    persons
        .iter()
        .enumerate()
        .filter(|(_, p)| {
            p.died_at.is_none()
                && p.insurance_start < config.insurance_start
                && p.insurance_end > config.insurance_end
                && p.third_effective_dose_date().is_some()
        })
        .map(|(i, _)| i)
        .collect()
}

/// Fully vaccinated (effective dose count == 2) persons who never went on to
/// receive a booster. Used as the "2-dose, unboostered" comparison pool.
pub fn two_dose_no_booster_people(persons: &[Person], config: &Config) -> Vec<usize> {
    persons
        .iter()
        .enumerate()
        .filter(|(_, p)| {
            p.died_at.is_none()
                && p.insurance_start < config.insurance_start
                && p.insurance_end > config.insurance_end
                && p.is_two_dose_no_booster()
        })
        .map(|(i, _)| i)
        .collect()
}

pub fn filter_group(
    persons: &[Person],
    vax_indices: &[usize],
    group: PeGroupName,
    config: &Config,
) -> Vec<usize> {
    vax_indices
        .iter()
        .copied()
        .filter(|&i| {
            let p = &persons[i];
            let vax_date = p.first_vax_date().unwrap();
            let pe = p.pe_before(vax_date, config.inj_mode);

            match group {
                PeGroupName::NeverPrescribed => {
                    !p.prescriptions.iter().any(|pr| pr.date < vax_date)
                }
                PeGroupName::ZeroPeSuspectible => {
                    pe == 0.0 && p.prescriptions.iter().any(|pr| pr.date < vax_date)
                }
                PeGroupName::ZeroPe => {
                    pe == 0.0
                        && cohort_eligibility::person_is_eligible(
                            p,
                            config.year_for_age(),
                            config.year_offset,
                        )
                }
                PeGroupName::OneToFiveHundredPe => pe >= 1.0 && pe < 500.0,
                PeGroupName::FiveHundredToFiveThousandPe => pe >= 500.0 && pe < 5000.0,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// CSV parsing internals
// ---------------------------------------------------------------------------

fn load_csv(path: &str, is_cpzp: bool, config: &Config) -> Vec<Person> {
    eprintln!("Reading {path} ...");

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(Path::new(path))
        .unwrap_or_else(|e| panic!("Cannot open {path}: {e}"));

    let headers = rdr.headers().unwrap().clone();
    let col = |name: &str| -> Option<usize> { headers.iter().position(|h| h == name) };

    let id_col = col("Id_pojistence").expect("missing Id_pojistence");
    let gender_col = col("Pohlavi").expect("missing Pohlavi");
    let birth_year_col = col("Rok_narozeni").expect("missing Rok_narozeni");
    let ins_start_col = col("Posledni_zahajeni_pojisteni").expect("missing ins start");
    let ins_end_col = col("Posledni_ukonceni_pojisteni").expect("missing ins end");
    let death_col = col("Datum_umrti").expect("missing Datum_umrti");
    let event_type_col = col("Typ_udalosti").expect("missing Typ_udalosti");
    let event_date_col = col("Datum_udalosti").expect("missing Datum_udalosti col");
    let atc_col = col("ATC_skupina");
    let forma_col = col("léková_forma_zkr").or_else(|| col("lekova_forma_zkr"));
    let pe_col = col("Prednison_equiv");
    let baleni_col = col("Pocet_baleni");
    let v_baleni_col = col("Pocet_v_baleni");
    let sila_col = col("síla").or_else(|| col("sila"));
    let spec_col = col("Specializace");
    let kod_udalosti_col = col("Kod_udalosti");
    let detail_udalosti_col = col("Detail_udalosti");

    // Accumulate raw rows grouped by person ID
    let mut groups: HashMap<String, Vec<RawRow>> = HashMap::new();

    for result in rdr.records() {
        let record = result.expect("bad CSV row");
        let get = |idx: usize| -> String { record.get(idx).unwrap_or("").to_string() };
        let get_opt = |idx: Option<usize>| -> Option<String> {
            idx.and_then(|i| {
                let v = record.get(i).unwrap_or("");
                if v.is_empty() || v == "NA" {
                    None
                } else {
                    Some(v.to_string())
                }
            })
        };

        let id_raw = get(id_col);
        if id_raw.is_empty() || id_raw == "NA" {
            continue;
        }

        let row = RawRow {
            gender: get(gender_col),
            birth_year: get_opt(Some(birth_year_col)).and_then(|s| s.parse().ok()),
            insurance_start: get_opt(Some(ins_start_col)).and_then(|s| parse_date(&s)),
            insurance_end: get_opt(Some(ins_end_col)).and_then(|s| parse_date(&s)),
            death_date: get_opt(Some(death_col)).and_then(|s| parse_date(&s)),
            event_type: get(event_type_col),
            event_date: get_opt(Some(event_date_col)).and_then(|s| parse_date(&s)),
            atc_skupina: get_opt(atc_col),
            lekova_forma_zkr: get_opt(forma_col),
            specializace: get_opt(spec_col),
            prednison_equiv: get_opt(pe_col).and_then(|s| s.parse().ok()),
            pocet_baleni: get_opt(baleni_col).and_then(|s| s.parse().ok()),
            pocet_v_baleni: get_opt(v_baleni_col).and_then(|s| s.parse().ok()),
            sila: get_opt(sila_col),
            kod_udalosti: get_opt(kod_udalosti_col),
            detail_udalosti: get_opt(detail_udalosti_col),
        };

        groups.entry(id_raw).or_default().push(row);
    }

    eprintln!("  parsed {} unique person IDs", groups.len());

    let day_offset = config.day_offset();
    let immuno_as_corticoid_500pe = config.immuno_as_corticoid_500pe;

    groups
        .into_iter()
        .filter_map(|(id_str, rows)| {
            build_person(&id_str, &rows, is_cpzp, day_offset, immuno_as_corticoid_500pe)
        })
        .collect()
}

fn build_person(
    id_str: &str,
    rows: &[RawRow],
    is_cpzp: bool,
    day_offset: chrono::Duration,
    immuno_as_corticoid_500pe: bool,
) -> Option<Person> {
    let first = &rows[0];

    let id = if is_cpzp {
        let n: f64 = id_str.parse().ok()?;
        PersonId::Numeric(n as u64)
    } else {
        PersonId::Text(id_str.to_string())
    };

    let gender = match first.gender.as_str() {
        "M" => Gender::Male,
        "Z" | "F" => Gender::Female,
        _ => return None,
    };

    let born_year = first.birth_year?;
    let insurance_start = first.insurance_start?;
    let insurance_end = first
        .insurance_end
        .unwrap_or(NaiveDate::from_ymd_opt(2050, 12, 31).unwrap());
    let died_at = first.death_date;

    let mut prescriptions = Vec::new();
    let mut vaccines: Vec<(NaiveDate, bool)> = Vec::new();

    for row in rows {
        let event_date = match row.event_date {
            Some(d) => d,
            None => continue,
        };

        if row.event_type.contains("předpis") || row.event_type.contains("predpis") {
            let atc = match &row.atc_skupina {
                Some(a) if a.starts_with("H02") || a.starts_with("L04") => a.clone(),
                _ => continue,
            };
            let is_immuno = atc.starts_with("L04");

            // When the immuno-as-corticoid override is on, every L04 prescription is
            // counted as a 500 mg prednison-equivalent corticoid prescription. This
            // is intentionally a coarse approximation so we get *some* picture of
            // the cohort once immunosuppressives are folded in.
            let pe_value = if immuno_as_corticoid_500pe && is_immuno {
                500.0
            } else {
                compute_pe(&row.prednison_equiv, &row.pocet_baleni, &row.pocet_v_baleni, &row.sila)
            };

            let spec = row
                .specializace
                .as_deref()
                .map(SpecialtyGroup::from_raw)
                .unwrap_or(SpecialtyGroup::Other);

            prescriptions.push(Prescription {
                date: event_date,
                prednison_equiv: pe_value,
                lekova_forma_zkr: row.lekova_forma_zkr.clone(),
                specialty: spec,
            });
        } else if row.event_type.contains("vakcinace") {
            let mut date = event_date;
            date -= day_offset;
            vaccines.push((date, is_janssen_row(row, is_cpzp)));
        }
    }

    // Sort injections chronologically, then assign chronological dose numbers
    // and Janssen-aware effective dose numbers: if the very first real
    // injection is Janssen it counts for 2 effective doses at once, and
    // every subsequent injection adds 1 effective dose as usual.
    vaccines.sort_by_key(|(date, _)| *date);
    let mut effective_count = 0u32;
    let vaccines: Vec<Vaccine> = vaccines
        .into_iter()
        .enumerate()
        .map(|(i, (date, is_janssen))| {
            effective_count += if i == 0 && is_janssen { 2 } else { 1 };
            Vaccine {
                date,
                dose_number: (i + 1) as u32,
                effective_dose_number: effective_count,
            }
        })
        .collect();

    Some(Person {
        id,
        gender,
        born_year,
        insurance_start,
        insurance_end,
        died_at,
        vaccines,
        prescriptions,
    })
}

#[cfg(test)]
mod janssen_tests {
    use super::*;

    fn row(kod: Option<&str>, detail: Option<&str>) -> RawRow {
        RawRow {
            gender: "M".to_string(),
            birth_year: Some(1980),
            insurance_start: None,
            insurance_end: None,
            death_date: None,
            event_type: "vakcinace".to_string(),
            event_date: None,
            atc_skupina: None,
            lekova_forma_zkr: None,
            specializace: None,
            prednison_equiv: None,
            pocet_baleni: None,
            pocet_v_baleni: None,
            sila: None,
            kod_udalosti: kod.map(str::to_string),
            detail_udalosti: detail.map(str::to_string),
        }
    }

    #[test]
    fn cpzp_detects_johnson_and_johnson_by_name() {
        let r = row(Some("COVID-19 - OČKOVÁNÍ - JOHNSON & JOHNSON"), None);
        assert!(is_janssen_row(&r, true));
    }

    #[test]
    fn cpzp_other_vaccines_are_not_janssen() {
        let r = row(Some("COVID-19 - OČKOVÁNÍ - COMIRNATY"), None);
        assert!(!is_janssen_row(&r, true));
        let r_none = row(None, None);
        assert!(!is_janssen_row(&r_none, true));
    }

    #[test]
    fn ozp_detects_janssen_by_performance_code() {
        assert!(is_janssen_row(&row(None, Some("99933")), false));
        assert!(is_janssen_row(&row(None, Some("99939")), false));
    }

    #[test]
    fn ozp_other_codes_are_not_janssen() {
        assert!(!is_janssen_row(&row(None, Some("99912")), false));
        assert!(!is_janssen_row(&row(None, None), false));
    }
}

fn compute_pe(
    prednison_equiv: &Option<f64>,
    pocet_baleni: &Option<f64>,
    pocet_v_baleni: &Option<f64>,
    sila: &Option<String>,
) -> f64 {
    let pe = match prednison_equiv {
        Some(v) => *v,
        None => return 0.0,
    };
    let bal = match pocet_baleni {
        Some(v) => *v,
        None => return 0.0,
    };
    let vbal = match pocet_v_baleni {
        Some(v) => *v,
        None => return 0.0,
    };
    let s = match sila {
        Some(s) if !s.is_empty() => {
            let cleaned = s.replace("MG", "").replace(",", ".").replace("/ML", "");
            match cleaned.parse::<f64>() {
                Ok(v) => v,
                Err(_) => return 0.0,
            }
        }
        _ => return 0.0,
    };

    pe * bal * vbal * s
}

fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}
