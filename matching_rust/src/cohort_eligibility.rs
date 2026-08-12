use chrono::{Duration, NaiveDate};

use crate::types::{AgeCohort, Person};

/// First eligible matching date: opening of the oldest part of each original
/// (coarser) age cohort plus three days.
pub fn base_eligibility_date(cohort: AgeCohort) -> Option<NaiveDate> {
    let (month, day) = match cohort {
        AgeCohort::Age12to15 => (7, 4),
        AgeCohort::Age16to22 | AgeCohort::Age23to29 => (6, 7),
        AgeCohort::Age30to39 | AgeCohort::Age40to49 => (5, 13),
        AgeCohort::Age50to59 => (4, 27),
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
            NaiveDate::from_ymd_opt(2021, 5, 13)
        );
        assert_eq!(
            base_eligibility_date(AgeCohort::Age40to49),
            NaiveDate::from_ymd_opt(2021, 5, 13)
        );
        assert_eq!(
            base_eligibility_date(AgeCohort::Age50to59),
            NaiveDate::from_ymd_opt(2021, 4, 27)
        );
        assert_eq!(base_eligibility_date(AgeCohort::Irrelevant), None);
    }

    #[test]
    fn shifts_threshold_like_vaccination_dates() {
        assert_eq!(
            eligibility_date(AgeCohort::Age30to39, 3),
            NaiveDate::from_ymd_opt(2018, 5, 14)
        );
        assert_eq!(
            eligibility_date(AgeCohort::Age30to39, -1),
            NaiveDate::from_ymd_opt(2022, 5, 13)
        );
    }
}
