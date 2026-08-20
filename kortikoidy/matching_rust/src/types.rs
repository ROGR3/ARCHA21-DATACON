use chrono::NaiveDate;
use serde::Serialize;
use std::fmt;

// ---------------------------------------------------------------------------
// PersonId – CPZP uses numeric IDs, OZP uses string IDs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PersonId {
    Numeric(u64),
    Text(String),
}

impl fmt::Display for PersonId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PersonId::Numeric(n) => write!(f, "{n}"),
            PersonId::Text(s) => write!(f, "{s}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Gender
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Gender {
    Male,
    Female,
}

// ---------------------------------------------------------------------------
// AgeCohort – mirrors the Python AgeCohort enum
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum AgeCohort {
    #[serde(rename = "12-15")]
    Age12to15,
    #[serde(rename = "16-22")]
    Age16to22,
    #[serde(rename = "23-29")]
    Age23to29,
    #[serde(rename = "30-39")]
    Age30to39,
    #[serde(rename = "40-49")]
    Age40to49,
    #[serde(rename = "50-59")]
    Age50to59,
    #[serde(rename = "irrelevant")]
    Irrelevant,
}

impl AgeCohort {
    pub fn from_age(age: i32) -> Self {
        match age {
            12..=15 => AgeCohort::Age12to15,
            16..=22 => AgeCohort::Age16to22,
            23..=29 => AgeCohort::Age23to29,
            30..=39 => AgeCohort::Age30to39,
            40..=49 => AgeCohort::Age40to49,
            50..=59 => AgeCohort::Age50to59,
            _ => AgeCohort::Irrelevant,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            AgeCohort::Age12to15 => "12-15",
            AgeCohort::Age16to22 => "16-22",
            AgeCohort::Age23to29 => "23-29",
            AgeCohort::Age30to39 => "30-39",
            AgeCohort::Age40to49 => "40-49",
            AgeCohort::Age50to59 => "50-59",
            AgeCohort::Irrelevant => "irrelevant",
        }
    }

    pub const ALL: [AgeCohort; 7] = [
        AgeCohort::Age12to15,
        AgeCohort::Age16to22,
        AgeCohort::Age23to29,
        AgeCohort::Age30to39,
        AgeCohort::Age40to49,
        AgeCohort::Age50to59,
        AgeCohort::Irrelevant,
    ];
}

// ---------------------------------------------------------------------------
// PeRange – prednison-equivalent category (25 mg buckets up to 5000)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PeRange {
    ZeroPe,
    ZeroNoPre,
    ZeroPeSuspectible,
    Between { low: u32, high: u32 },
    MoreThan5000,
}

impl PeRange {
    pub fn from_pe(pe: f64) -> Self {
        if pe == 0.0 {
            return PeRange::ZeroPe;
        }
        if pe >= 5000.0 {
            return PeRange::MoreThan5000;
        }
        let step = 25u32;
        let low = (pe as u32 / step) * step;
        PeRange::Between {
            low,
            high: low + step,
        }
    }
}

// ---------------------------------------------------------------------------
// PE group names – which subgroup of vax people we analyse
// ---------------------------------------------------------------------------

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PeGroupName {
    NeverPrescribed,
    ZeroPeSuspectible,
    ZeroPe,
    OneToFiveHundredPe,
    FiveHundredToFiveThousandPe,
}

impl PeGroupName {
    pub fn label(&self) -> &'static str {
        match self {
            PeGroupName::NeverPrescribed => "NEVER_PRESCRIBED",
            PeGroupName::ZeroPeSuspectible => "ZERO_PE_SUSPECTIBLE",
            PeGroupName::ZeroPe => "0_PE",
            PeGroupName::OneToFiveHundredPe => "1_to_500_PE",
            PeGroupName::FiveHundredToFiveThousandPe => "500_to_5000_PE",
        }
    }

    pub fn is_zero_pe_group(&self) -> bool {
        matches!(
            self,
            PeGroupName::ZeroPe | PeGroupName::NeverPrescribed | PeGroupName::ZeroPeSuspectible
        )
    }
}

// ---------------------------------------------------------------------------
// SpecialtyGroup – doctor specialty buckets
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum SpecialtyGroup {
    #[serde(rename = "ortopedie_chirurgie")]
    OrtopedieChirurgie,
    #[serde(rename = "revmatologie")]
    Revmatologie,
    #[serde(rename = "neurologie")]
    Neurologie,
    #[serde(rename = "praktik")]
    Praktik,
    #[serde(rename = "gastroenterologie")]
    Gastroenterologie,
    #[serde(rename = "pneumo_ftizeo")]
    PneumoFtizeo,
    #[serde(rename = "interna")]
    Interna,
    #[serde(rename = "other")]
    Other,
}

impl SpecialtyGroup {
    pub fn from_raw(raw: &str) -> Self {
        let lower = raw.to_lowercase();
        if lower.contains("ortopedie")
            || lower == "chirurgie"
            || lower.contains("úrazová chirurgie")
            || lower.contains("hrudní chirurgie")
            || lower.contains("neurochirurgie")
            || lower.contains("plastická chirurgie")
        {
            SpecialtyGroup::OrtopedieChirurgie
        } else if lower.contains("revmatologie") {
            SpecialtyGroup::Revmatologie
        } else if lower == "neurologie" {
            SpecialtyGroup::Neurologie
        } else if lower.contains("praktické lékařství") || lower.contains("praktick") {
            SpecialtyGroup::Praktik
        } else if lower.contains("gastroenterologie") {
            SpecialtyGroup::Gastroenterologie
        } else if lower.contains("pneumologie") || lower.contains("ftizeologie") {
            SpecialtyGroup::PneumoFtizeo
        } else if lower.contains("vnitřní lékařství") || lower.contains("interna") {
            SpecialtyGroup::Interna
        } else {
            SpecialtyGroup::Other
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            SpecialtyGroup::OrtopedieChirurgie => "ortopedie_chirurgie",
            SpecialtyGroup::Revmatologie => "revmatologie",
            SpecialtyGroup::Neurologie => "neurologie",
            SpecialtyGroup::Praktik => "praktik",
            SpecialtyGroup::Gastroenterologie => "gastroenterologie",
            SpecialtyGroup::PneumoFtizeo => "pneumo_ftizeo",
            SpecialtyGroup::Interna => "interna",
            SpecialtyGroup::Other => "other",
        }
    }

    pub const ALL: [SpecialtyGroup; 8] = [
        SpecialtyGroup::OrtopedieChirurgie,
        SpecialtyGroup::Revmatologie,
        SpecialtyGroup::Neurologie,
        SpecialtyGroup::Praktik,
        SpecialtyGroup::Gastroenterologie,
        SpecialtyGroup::PneumoFtizeo,
        SpecialtyGroup::Interna,
        SpecialtyGroup::Other,
    ];
}

// ---------------------------------------------------------------------------
// Core domain structs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Prescription {
    pub date: NaiveDate,
    pub prednison_equiv: f64,
    pub lekova_forma_zkr: Option<String>,
    pub specialty: SpecialtyGroup,
}

#[derive(Debug, Clone)]
pub struct Vaccine {
    pub date: NaiveDate,
    /// Chronological order among this person's real injections (1-indexed).
    #[allow(dead_code)]
    pub dose_number: u32,
    /// Running total of "effective" doses after this injection. Equal to
    /// `dose_number` for all vaccines except when the person's very first
    /// injection is Janssen (single-shot, counts as 2 effective doses): in
    /// that case this jumps straight to 2 for the first shot, and every
    /// subsequent real injection is one effective dose higher than usual.
    pub effective_dose_number: u32,
}

#[derive(Debug, Clone)]
pub struct Person {
    pub id: PersonId,
    pub gender: Gender,
    pub born_year: i32,
    pub insurance_start: NaiveDate,
    pub insurance_end: NaiveDate,
    pub died_at: Option<NaiveDate>,
    pub vaccines: Vec<Vaccine>,
    pub prescriptions: Vec<Prescription>,
}

impl Person {
    pub fn age_cohort(&self, reference_year: i32) -> AgeCohort {
        AgeCohort::from_age(reference_year - self.born_year)
    }

    pub fn first_vax_date(&self) -> Option<NaiveDate> {
        self.vaccines.first().map(|v| v.date)
    }

    /// Date of the injection where the effective dose count first reaches 3
    /// (the booster), accounting for Janssen's single-shot-counts-as-2 rule.
    /// `None` if the person never reached an effective 3rd dose.
    pub fn third_effective_dose_date(&self) -> Option<NaiveDate> {
        self.vaccines
            .iter()
            .find(|v| v.effective_dose_number >= 3)
            .map(|v| v.date)
    }

    /// True if the person is fully vaccinated (effective dose count == 2)
    /// and never received a further injection afterwards. Covers both a
    /// single Janssen shot and a normal two-shot primary series.
    pub fn is_two_dose_no_booster(&self) -> bool {
        self.vaccines
            .last()
            .is_some_and(|v| v.effective_dose_number == 2)
    }

    #[cfg(test)]
    fn test_person(vaccines: Vec<Vaccine>) -> Person {
        Person {
            id: PersonId::Numeric(1),
            gender: Gender::Male,
            born_year: 1980,
            insurance_start: NaiveDate::from_ymd_opt(2015, 1, 1).unwrap(),
            insurance_end: NaiveDate::from_ymd_opt(2050, 12, 31).unwrap(),
            died_at: None,
            vaccines,
            prescriptions: Vec::new(),
        }
    }

    /// Sum of prednison_equiv in the 365 days BEFORE `anchor`.
    pub fn pe_before(&self, anchor: NaiveDate, inj_mode: InjMode) -> f64 {
        let start = anchor - chrono::Duration::days(365);
        self.prescriptions
            .iter()
            .filter(|p| inj_mode.matches(p) && p.date > start && p.date < anchor)
            .map(|p| p.prednison_equiv)
            .sum()
    }

    /// Sum of prednison_equiv in the 365 days AFTER `anchor`.
    pub fn pe_after(&self, anchor: NaiveDate, inj_mode: InjMode) -> f64 {
        let end = anchor + chrono::Duration::days(365);
        self.prescriptions
            .iter()
            .filter(|p| inj_mode.matches(p) && p.date > anchor && p.date < end)
            .map(|p| p.prednison_equiv)
            .sum()
    }

    #[allow(dead_code)]
    pub fn has_prescriptions_before(&self, anchor: NaiveDate) -> bool {
        self.prescriptions.iter().any(|p| p.date < anchor)
    }

    pub fn pe_before_by_specialty(
        &self,
        anchor: NaiveDate,
        inj_mode: InjMode,
    ) -> HashMap<SpecialtyGroup, f64> {
        let start = anchor - chrono::Duration::days(365);
        let mut map = HashMap::new();
        for p in &self.prescriptions {
            if inj_mode.matches(p) && p.date > start && p.date < anchor {
                *map.entry(p.specialty).or_default() += p.prednison_equiv;
            }
        }
        map
    }

    pub fn pe_after_by_specialty(
        &self,
        anchor: NaiveDate,
        inj_mode: InjMode,
    ) -> HashMap<SpecialtyGroup, f64> {
        let end = anchor + chrono::Duration::days(365);
        let mut map = HashMap::new();
        for p in &self.prescriptions {
            if inj_mode.matches(p) && p.date > anchor && p.date < end {
                *map.entry(p.specialty).or_default() += p.prednison_equiv;
            }
        }
        map
    }
}

// ---------------------------------------------------------------------------
// InjMode – controls which prescriptions are included
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub enum InjMode {
    InjOnly,
    NonInj,
    All,
}

impl InjMode {
    pub fn matches(&self, pr: &Prescription) -> bool {
        match self {
            InjMode::All => true,
            InjMode::InjOnly => pr
                .lekova_forma_zkr
                .as_deref()
                .is_some_and(|s| s.starts_with("INJ")),
            InjMode::NonInj => !pr
                .lekova_forma_zkr
                .as_deref()
                .is_some_and(|s| s.starts_with("INJ")),
        }
    }
}

// ---------------------------------------------------------------------------
// Type aliases for the large nested maps
// ---------------------------------------------------------------------------

use std::collections::HashMap;

/// pe_map[anchor_date][pe_range][age_cohort][gender] → list of novax person indices
pub type PeMap = HashMap<NaiveDate, HashMap<PeRange, HashMap<AgeCohort, HashMap<Gender, Vec<usize>>>>>;

#[cfg(test)]
mod booster_tests {
    use super::*;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    #[test]
    fn normal_vaccine_reaches_effective_dose_3_on_third_shot() {
        let vaccines = vec![
            Vaccine { date: d(2021, 1, 1), dose_number: 1, effective_dose_number: 1 },
            Vaccine { date: d(2021, 2, 1), dose_number: 2, effective_dose_number: 2 },
            Vaccine { date: d(2021, 9, 1), dose_number: 3, effective_dose_number: 3 },
        ];
        let p = Person::test_person(vaccines);
        assert_eq!(p.third_effective_dose_date(), Some(d(2021, 9, 1)));
        assert!(!p.is_two_dose_no_booster());
    }

    #[test]
    fn janssen_first_shot_counts_as_two_effective_doses() {
        // A single Janssen shot already counts as effective dose 2; the next
        // real injection (if any) becomes effective dose 3 == booster.
        let vaccines = vec![
            Vaccine { date: d(2021, 3, 1), dose_number: 1, effective_dose_number: 2 },
            Vaccine { date: d(2021, 10, 1), dose_number: 2, effective_dose_number: 3 },
        ];
        let p = Person::test_person(vaccines);
        assert_eq!(p.third_effective_dose_date(), Some(d(2021, 10, 1)));
    }

    #[test]
    fn janssen_only_with_no_further_shot_is_two_dose_no_booster() {
        let vaccines = vec![Vaccine {
            date: d(2021, 3, 1),
            dose_number: 1,
            effective_dose_number: 2,
        }];
        let p = Person::test_person(vaccines);
        assert!(p.is_two_dose_no_booster());
        assert_eq!(p.third_effective_dose_date(), None);
    }

    #[test]
    fn two_normal_doses_with_no_booster_is_two_dose_no_booster() {
        let vaccines = vec![
            Vaccine { date: d(2021, 1, 1), dose_number: 1, effective_dose_number: 1 },
            Vaccine { date: d(2021, 2, 1), dose_number: 2, effective_dose_number: 2 },
        ];
        let p = Person::test_person(vaccines);
        assert!(p.is_two_dose_no_booster());
        assert_eq!(p.third_effective_dose_date(), None);
    }

    #[test]
    fn never_vaccinated_has_no_third_dose_and_is_not_two_dose() {
        let p = Person::test_person(Vec::new());
        assert_eq!(p.third_effective_dose_date(), None);
        assert!(!p.is_two_dose_no_booster());
    }
}
