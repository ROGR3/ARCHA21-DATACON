use chrono::{Duration, NaiveDate};

use crate::types::{AgeCohort, Person};

/// First eligible matching date: ČSÚ registration opening for the oldest
/// member of each fine cohort plus three days (Tab. 9, Demografie 2022).
pub fn base_eligibility_date(cohort: AgeCohort) -> Option<NaiveDate> {
    let (month, day) = match cohort {
        // 1. 7. 2021 (12+) + 3
        AgeCohort::Age12to15 => (7, 4),
        // 4. 6. 2021 (16+) + 3
        AgeCohort::Age16to22 | AgeCohort::Age23to29 => (6, 7),
        // 24. 5. 2021 (35+) + 3
        AgeCohort::Age30to39 => (5, 27),
        // 10. 5. 2021 (45+) + 3
        AgeCohort::Age40to49 => (5, 13),
        // 28. 4. 2021 (55+) + 3
        AgeCohort::Age50to59 => (5, 1),
        AgeCohort::Irrelevant => return None,
    };

    NaiveDate::from_ymd_opt(2021, month, day)
}

/// Shift the eligibility date exactly like vaccination dates are shifted.
pub fn eligibility_date(cohort: AgeCohort, year_offset: i32) -> Option<NaiveDate> {
    base_eligibility_date(cohort).map(|date| date - Duration::days(i64::from(year_offset) * 365))
}

pub fn person_is_eligible(person: &Person, reference_year: i32, year_offset: i32) -> bool {
    let cohort = person.age_cohort(reference_year);
    let Some(threshold) = eligibility_date(cohort, year_offset) else {
        return false;
    };

    person
        .first_vax_date()
        .is_some_and(|vax_date| vax_date >= threshold)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_fine_cohorts_to_oldest_part_openings_plus_three_days() {
        assert_eq!(
            base_eligibility_date(AgeCohort::Age12to15),
            NaiveDate::from_ymd_opt(2021, 7, 4)
        );
        assert_eq!(
            base_eligibility_date(AgeCohort::Age16to22),
            NaiveDate::from_ymd_opt(2021, 6, 7)
        );
        assert_eq!(
            base_eligibility_date(AgeCohort::Age23to29),
            NaiveDate::from_ymd_opt(2021, 6, 7)
        );
        assert_eq!(
            base_eligibility_date(AgeCohort::Age30to39),
            NaiveDate::from_ymd_opt(2021, 5, 27)
        );
        assert_eq!(
            base_eligibility_date(AgeCohort::Age40to49),
            NaiveDate::from_ymd_opt(2021, 5, 13)
        );
        assert_eq!(
            base_eligibility_date(AgeCohort::Age50to59),
            NaiveDate::from_ymd_opt(2021, 5, 1)
        );
        assert_eq!(base_eligibility_date(AgeCohort::Irrelevant), None);
    }

    #[test]
    fn shifts_threshold_like_vaccination_dates() {
        assert_eq!(
            eligibility_date(AgeCohort::Age30to39, 3),
            NaiveDate::from_ymd_opt(2018, 5, 28)
        );
        assert_eq!(
            eligibility_date(AgeCohort::Age30to39, -1),
            NaiveDate::from_ymd_opt(2022, 5, 27)
        );
    }
}
