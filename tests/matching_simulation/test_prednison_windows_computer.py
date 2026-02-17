"""Tests for PrednisonWindowsComputer.

These tests verify the theoretical correctness of the PE (prednison-equivalent)
window computation. The computer builds a 4-level nested map:

    PeMap[anchor_date][PE_category][AgeCohort][Gender] → [person_ids]

For each anchor date and each unvaccinated person, the cumulative PE from
non-injection prescriptions within the preceding 365-day window determines
which PE category the person falls into.

THEORETICAL NOTE — Boundary consistency:
    The PrednisonWindowsComputer uses bisect on [pr.date, pr.date + 365] to
    find which anchors a prescription contributes to. This produces a CLOSED
    interval: prescription P contributes to anchor A when P.date <= A <= P.date + 365.

    In contrast, sum_before_date_pe_for_person uses an OPEN interval:
    P.date > A - 365 and P.date < A  →  (A - 365, A).

    This means the PE window computer INCLUDES prescriptions on the exact anchor
    date and exactly 365 days before, while sum_before EXCLUDES them. Tests below
    document this behavior.
"""

from datetime import date, timedelta

from common.constants.objects import Gender
from common.matching_simulation.prednison_windows_computer import (
    PrednisonWindowsComputer,
)
from common.matching_simulation.utils import (
    PREDNISON_EQUIV_CATEGORY,
    AgeCohort,
    AgeCohortCalculator,
    MatchingAnalysisConfig,
    PeMap,
    from_prednison_equiv,
)

from .conftest import make_person, make_prescription


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(year_offset: int = 0) -> MatchingAnalysisConfig:
    return MatchingAnalysisConfig(
        pojistovna="test",
        zacatek_pojisteni=date(2020, 1, 1),
        konec_pojisteni=date(2023, 1, 1),
        year_offset=year_offset,
        use_local_cache=False,
    )


def _build_pe_map(
    people: list,
    anchor_dates: list[date],
    config: MatchingAnalysisConfig | None = None,
) -> PeMap:
    """Run the PrednisonWindowsComputer and return the PeMap."""
    config = config or _make_config()
    calc = AgeCohortCalculator(config)
    computer = PrednisonWindowsComputer(config, calc)
    return computer.get_prednison_windows(people, anchor_dates)


def _person_in_map(
    pe_map: PeMap, anchor: date, pe_cat, cohort: AgeCohort, gender: Gender, pid
):
    """Check whether a person ID is found at the expected position in the map."""
    try:
        return pid in pe_map[anchor][pe_cat][cohort][gender]
    except KeyError:
        return False


# ===================================================================
# Person with NO prescriptions
# ===================================================================


class TestNoPrescriptions:
    """A person with no prescriptions at all should be ZERO_PE + ZERO_NO_PRE
    for every anchor date."""

    def test_classified_as_zero_pe_for_all_anchors(self):
        anchors = [date(2021, 6, 1), date(2021, 6, 2), date(2021, 6, 3)]
        person = make_person(pid=1, prescriptions=[])
        pe_map = _build_pe_map([person], anchors)

        for a in anchors:
            assert _person_in_map(
                pe_map,
                a,
                PREDNISON_EQUIV_CATEGORY.ZERO_PE,
                AgeCohort._30_49,
                Gender.MALE,
                1,
            )

    def test_classified_as_zero_no_pre_for_all_anchors(self):
        anchors = [date(2021, 6, 1), date(2021, 6, 2)]
        person = make_person(pid=1, prescriptions=[])
        pe_map = _build_pe_map([person], anchors)

        for a in anchors:
            assert _person_in_map(
                pe_map,
                a,
                PREDNISON_EQUIV_CATEGORY.ZERO_NO_PRE,
                AgeCohort._30_49,
                Gender.MALE,
                1,
            )

    def test_not_classified_as_suspectible(self):
        anchors = [date(2021, 6, 1)]
        person = make_person(pid=1, prescriptions=[])
        pe_map = _build_pe_map([person], anchors)

        assert not _person_in_map(
            pe_map,
            anchors[0],
            PREDNISON_EQUIV_CATEGORY.ZERO_PE_SUSPECTIBLE,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )


# ===================================================================
# Person with ONLY injection prescriptions
# ===================================================================


class TestInjectionOnlyPrescriptions:
    """Injection prescriptions are filtered out of the PE accumulation.
    The person should be ZERO_PE. But has_prescriptions_before_date checks
    ALL prescriptions (including injections), so a person whose only
    prescriptions are injections BEFORE the anchor date should be
    ZERO_PE_SUSPECTIBLE (not ZERO_NO_PRE)."""

    def test_injection_only_person_is_zero_pe(self):
        anchors = [date(2021, 6, 1)]
        person = make_person(
            pid=1,
            prescriptions=[
                make_prescription(date(2021, 3, 1), pe=500.0, injection=True),
            ],
        )
        pe_map = _build_pe_map([person], anchors)

        assert _person_in_map(
            pe_map,
            anchors[0],
            PREDNISON_EQUIV_CATEGORY.ZERO_PE,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )

    def test_injection_before_anchor_makes_suspectible(self):
        """has_prescriptions_before_date considers injections too, so this
        person should be ZERO_PE_SUSPECTIBLE rather than ZERO_NO_PRE."""
        anchors = [date(2021, 6, 1)]
        person = make_person(
            pid=1,
            prescriptions=[
                make_prescription(date(2021, 3, 1), pe=500.0, injection=True),
            ],
        )
        pe_map = _build_pe_map([person], anchors)

        assert _person_in_map(
            pe_map,
            anchors[0],
            PREDNISON_EQUIV_CATEGORY.ZERO_PE_SUSPECTIBLE,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )

    def test_injection_after_anchor_means_no_pre(self):
        """Injection only AFTER anchor → has_prescriptions_before_date=False → ZERO_NO_PRE."""
        anchors = [date(2021, 6, 1)]
        person = make_person(
            pid=1,
            prescriptions=[
                make_prescription(date(2021, 9, 1), pe=500.0, injection=True),
            ],
        )
        pe_map = _build_pe_map([person], anchors)

        assert _person_in_map(
            pe_map,
            anchors[0],
            PREDNISON_EQUIV_CATEGORY.ZERO_NO_PRE,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )


# ===================================================================
# Single prescription — basic window logic
# ===================================================================


class TestSinglePrescriptionWindow:
    """Verify that a prescription contributes PE to anchors within its
    365-day forward window and NOT to anchors outside it."""

    def test_anchor_inside_window_gets_pe(self):
        pr_date = date(2021, 3, 1)
        anchor = date(2021, 6, 1)  # ~92 days after prescription
        person = make_person(
            pid=1, prescriptions=[make_prescription(pr_date, pe=100.0)]
        )
        pe_map = _build_pe_map([person], [anchor])

        expected_cat = from_prednison_equiv(100.0)
        assert _person_in_map(
            pe_map,
            anchor,
            expected_cat,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )

    def test_anchor_outside_window_gets_zero(self):
        pr_date = date(2020, 1, 1)
        anchor = date(2021, 6, 1)  # >365 days after prescription
        person = make_person(
            pid=1, prescriptions=[make_prescription(pr_date, pe=100.0)]
        )
        pe_map = _build_pe_map([person], [anchor])

        assert _person_in_map(
            pe_map,
            anchor,
            PREDNISON_EQUIV_CATEGORY.ZERO_PE,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )

    def test_anchor_before_prescription_gets_zero(self):
        pr_date = date(2021, 9, 1)
        anchor = date(2021, 6, 1)  # before prescription
        person = make_person(
            pid=1, prescriptions=[make_prescription(pr_date, pe=100.0)]
        )
        pe_map = _build_pe_map([person], [anchor])

        assert _person_in_map(
            pe_map,
            anchor,
            PREDNISON_EQUIV_CATEGORY.ZERO_PE,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )


# ===================================================================
# Boundary conditions — the critical tests
# ===================================================================


class TestWindowBoundaries:
    """Boundary behavior of the PE window computer.

    The computer uses bisect_left / bisect_right on [pr.date, pr.date + 365]:
      - bisect_left(anchors, pr.date)  → first anchor >= pr.date
      - bisect_right(anchors, pr.date + 365)  → first anchor > pr.date + 365

    Consequence: anchor A sees prescription P when  P.date <= A <= P.date + 365.

    This differs from sum_before_date_pe_for_person which uses strict inequalities:
    P.date > A - 365 and P.date < A.
    """

    def test_prescription_on_exact_anchor_date_is_included(self):
        """When pr.date == anchor, the PE window computer INCLUDES it.

        NOTE: sum_before_date_pe_for_person would EXCLUDE this (pr.date < ddate
        is False when they're equal). This is a known boundary inconsistency
        between the two codepaths.
        """
        anchor = date(2021, 6, 1)
        pr_date = date(2021, 6, 1)
        person = make_person(
            pid=1, prescriptions=[make_prescription(pr_date, pe=100.0)]
        )
        pe_map = _build_pe_map([person], [anchor])

        expected_cat = from_prednison_equiv(100.0)
        assert _person_in_map(
            pe_map,
            anchor,
            expected_cat,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        ), (
            "PE window computer should include a prescription whose date equals "
            "the anchor date (closed interval on left)"
        )

    def test_prescription_exactly_365_days_before_anchor_is_included(self):
        """When anchor == pr.date + 365, bisect_right returns index > anchor,
        so the anchor IS included.

        NOTE: sum_before_date_pe_for_person would EXCLUDE this (pr.date > A - 365
        is False when pr.date == A - 365).
        """
        anchor = date(2021, 6, 1)
        pr_date = anchor - timedelta(days=365)
        person = make_person(
            pid=1, prescriptions=[make_prescription(pr_date, pe=100.0)]
        )
        pe_map = _build_pe_map([person], [anchor])

        expected_cat = from_prednison_equiv(100.0)
        assert _person_in_map(
            pe_map,
            anchor,
            expected_cat,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        ), (
            "PE window computer should include a prescription exactly 365 days "
            "before the anchor (closed interval on right: anchor == pr.date + 365)"
        )

    def test_prescription_366_days_before_anchor_is_excluded(self):
        """pr.date + 365 < anchor → outside the window."""
        anchor = date(2021, 6, 1)
        pr_date = anchor - timedelta(days=366)
        person = make_person(
            pid=1, prescriptions=[make_prescription(pr_date, pe=100.0)]
        )
        pe_map = _build_pe_map([person], [anchor])

        assert _person_in_map(
            pe_map,
            anchor,
            PREDNISON_EQUIV_CATEGORY.ZERO_PE,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )

    def test_prescription_one_day_after_anchor_is_excluded(self):
        """pr.date > anchor → bisect_left returns index past anchor."""
        anchor = date(2021, 6, 1)
        pr_date = date(2021, 6, 2)
        person = make_person(
            pid=1, prescriptions=[make_prescription(pr_date, pe=100.0)]
        )
        pe_map = _build_pe_map([person], [anchor])

        assert _person_in_map(
            pe_map,
            anchor,
            PREDNISON_EQUIV_CATEGORY.ZERO_PE,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )


# ===================================================================
# Multiple prescriptions — accumulation
# ===================================================================


class TestPeAccumulation:
    """When multiple prescriptions fall within the 365-day window for the same
    anchor date, their PE values should accumulate (sum)."""

    def test_two_prescriptions_sum(self):
        anchors = [date(2021, 6, 1)]
        person = make_person(
            pid=1,
            prescriptions=[
                make_prescription(date(2021, 3, 1), pe=60.0),
                make_prescription(date(2021, 4, 1), pe=40.0),
            ],
        )
        pe_map = _build_pe_map([person], anchors)

        expected_cat = from_prednison_equiv(100.0)  # 60 + 40
        assert _person_in_map(
            pe_map,
            anchors[0],
            expected_cat,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )

    def test_only_in_window_prescriptions_accumulate(self):
        """Out-of-window prescriptions should not contribute."""
        anchor = date(2021, 6, 1)
        person = make_person(
            pid=1,
            prescriptions=[
                make_prescription(date(2021, 3, 1), pe=60.0),  # in window
                make_prescription(date(2020, 1, 1), pe=9999.0),  # out of window
            ],
        )
        pe_map = _build_pe_map([person], [anchor])

        expected_cat = from_prednison_equiv(60.0)
        assert _person_in_map(
            pe_map,
            anchor,
            expected_cat,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )

    def test_injections_not_accumulated(self):
        anchor = date(2021, 6, 1)
        person = make_person(
            pid=1,
            prescriptions=[
                make_prescription(date(2021, 3, 1), pe=60.0),
                make_prescription(date(2021, 4, 1), pe=1000.0, injection=True),
            ],
        )
        pe_map = _build_pe_map([person], [anchor])

        expected_cat = from_prednison_equiv(60.0)
        assert _person_in_map(
            pe_map,
            anchor,
            expected_cat,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )

    def test_accumulation_pushes_to_higher_bucket(self):
        """Multiple small prescriptions should accumulate to push person
        into a higher PE bucket."""
        anchor = date(2021, 6, 1)
        person = make_person(
            pid=1,
            prescriptions=[
                make_prescription(date(2021, 3, 1), pe=15.0),
                make_prescription(date(2021, 4, 1), pe=15.0),
            ],
        )
        pe_map = _build_pe_map([person], [anchor])

        # 15 + 15 = 30 → BETWEEN_25_AND_50
        expected_cat = from_prednison_equiv(30.0)
        assert _person_in_map(
            pe_map,
            anchor,
            expected_cat,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )


# ===================================================================
# Window sliding over anchor dates
# ===================================================================


class TestWindowSliding:
    """As anchor dates progress, prescriptions should enter and exit
    the 365-day lookback window."""

    def test_prescription_enters_window(self):
        """A prescription issued on 2021-06-01 should NOT appear in the window
        for 2021-05-31 but SHOULD appear for 2021-06-01."""
        pr_date = date(2021, 6, 1)
        anchors = [date(2021, 5, 31), date(2021, 6, 1)]
        person = make_person(
            pid=1, prescriptions=[make_prescription(pr_date, pe=100.0)]
        )
        pe_map = _build_pe_map([person], anchors)

        cat = from_prednison_equiv(100.0)
        assert not _person_in_map(
            pe_map,
            date(2021, 5, 31),
            cat,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )
        assert _person_in_map(
            pe_map,
            date(2021, 6, 1),
            cat,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )

    def test_prescription_exits_window(self):
        """A prescription issued on 2021-01-01 should still be in the window
        for 2022-01-01 (exactly 365 days later) but NOT for 2022-01-02."""
        pr_date = date(2021, 1, 1)
        day_365 = date(2022, 1, 1)
        day_366 = date(2022, 1, 2)
        anchors = [day_365, day_366]
        person = make_person(
            pid=1, prescriptions=[make_prescription(pr_date, pe=100.0)]
        )
        pe_map = _build_pe_map([person], anchors)

        cat = from_prednison_equiv(100.0)
        # At exactly 365 days: still included (closed interval)
        assert _person_in_map(
            pe_map,
            day_365,
            cat,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )
        # At 366 days: excluded
        assert _person_in_map(
            pe_map,
            day_366,
            PREDNISON_EQUIV_CATEGORY.ZERO_PE,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )


# ===================================================================
# Gender and age cohort partitioning
# ===================================================================


class TestDemographicPartitioning:
    """People should be placed in the correct age cohort x gender slot."""

    def test_male_and_female_separated(self):
        anchor = date(2021, 6, 1)
        male = make_person(pid=1, gender=Gender.MALE, prescriptions=[])
        female = make_person(pid=2, gender=Gender.FEMALE, prescriptions=[])
        pe_map = _build_pe_map([male, female], [anchor])

        cat = PREDNISON_EQUIV_CATEGORY.ZERO_PE
        cohort = AgeCohort._30_49
        assert _person_in_map(pe_map, anchor, cat, cohort, Gender.MALE, 1)
        assert _person_in_map(pe_map, anchor, cat, cohort, Gender.FEMALE, 2)
        assert not _person_in_map(pe_map, anchor, cat, cohort, Gender.FEMALE, 1)
        assert not _person_in_map(pe_map, anchor, cat, cohort, Gender.MALE, 2)

    def test_different_age_cohorts_separated(self):
        anchor = date(2021, 6, 1)
        young = make_person(pid=1, born_year=2021 - 14)  # 14 -> _12_15
        middle = make_person(pid=2, born_year=2021 - 35)  # 35 -> _30_49
        pe_map = _build_pe_map([young, middle], [anchor])

        cat = PREDNISON_EQUIV_CATEGORY.ZERO_PE
        assert _person_in_map(pe_map, anchor, cat, AgeCohort._12_15, Gender.MALE, 1)
        assert _person_in_map(pe_map, anchor, cat, AgeCohort._30_49, Gender.MALE, 2)
        assert not _person_in_map(pe_map, anchor, cat, AgeCohort._12_15, Gender.MALE, 2)
        assert not _person_in_map(pe_map, anchor, cat, AgeCohort._30_49, Gender.MALE, 1)


# ===================================================================
# Suspectible vs no_pre classification
# ===================================================================


class TestSuspectibleClassification:
    """When a person has prescriptions (any type) before the anchor but
    zero non-injection PE in the window -> ZERO_PE_SUSPECTIBLE.
    When no prescriptions at all before anchor -> ZERO_NO_PRE."""

    def test_prescription_before_anchor_but_outside_pe_window(self):
        """Person has a non-injection prescription far in the past (outside
        the 365-day PE window) -> 0 PE, but has_prescriptions_before_date=True
        -> should be ZERO_PE_SUSPECTIBLE."""
        anchor = date(2021, 6, 1)
        person = make_person(
            pid=1,
            prescriptions=[
                make_prescription(date(2019, 1, 1), pe=100.0),  # >2 years ago
            ],
        )
        pe_map = _build_pe_map([person], [anchor])

        assert _person_in_map(
            pe_map,
            anchor,
            PREDNISON_EQUIV_CATEGORY.ZERO_PE,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )
        assert _person_in_map(
            pe_map,
            anchor,
            PREDNISON_EQUIV_CATEGORY.ZERO_PE_SUSPECTIBLE,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )
        assert not _person_in_map(
            pe_map,
            anchor,
            PREDNISON_EQUIV_CATEGORY.ZERO_NO_PRE,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )

    def test_prescription_only_after_anchor(self):
        """Person has prescriptions but only AFTER the anchor -> not 'before',
        so should be ZERO_NO_PRE."""
        anchor = date(2021, 6, 1)
        person = make_person(
            pid=1,
            prescriptions=[make_prescription(date(2021, 9, 1), pe=100.0)],
        )
        pe_map = _build_pe_map([person], [anchor])

        assert _person_in_map(
            pe_map,
            anchor,
            PREDNISON_EQUIV_CATEGORY.ZERO_NO_PRE,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )


# ===================================================================
# Multiple people
# ===================================================================


class TestMultiplePeople:
    """Multiple people in the same PE bucket / cohort / gender should all
    appear in the output list."""

    def test_two_people_same_bucket(self):
        anchor = date(2021, 6, 1)
        p1 = make_person(
            pid=1,
            prescriptions=[make_prescription(date(2021, 3, 1), pe=10.0)],
        )
        p2 = make_person(
            pid=2,
            prescriptions=[make_prescription(date(2021, 4, 1), pe=15.0)],
        )
        pe_map = _build_pe_map([p1, p2], [anchor])

        cat_p1 = from_prednison_equiv(10.0)
        cat_p2 = from_prednison_equiv(15.0)
        # Both should be BETWEEN_0_AND_25
        assert cat_p1 == cat_p2
        assert _person_in_map(pe_map, anchor, cat_p1, AgeCohort._30_49, Gender.MALE, 1)
        assert _person_in_map(pe_map, anchor, cat_p2, AgeCohort._30_49, Gender.MALE, 2)

    def test_people_in_different_buckets(self):
        anchor = date(2021, 6, 1)
        low_pe = make_person(
            pid=1,
            prescriptions=[make_prescription(date(2021, 3, 1), pe=10.0)],
        )
        high_pe = make_person(
            pid=2,
            prescriptions=[make_prescription(date(2021, 3, 1), pe=500.0)],
        )
        pe_map = _build_pe_map([low_pe, high_pe], [anchor])

        cat_low = from_prednison_equiv(10.0)
        cat_high = from_prednison_equiv(500.0)
        assert cat_low != cat_high
        assert _person_in_map(pe_map, anchor, cat_low, AgeCohort._30_49, Gender.MALE, 1)
        assert _person_in_map(
            pe_map, anchor, cat_high, AgeCohort._30_49, Gender.MALE, 2
        )


# ===================================================================
# Output structure integrity
# ===================================================================


class TestOutputStructure:
    """Verify the output is a proper PeMap with lists (not sets)."""

    def test_output_values_are_lists(self):
        anchor = date(2021, 6, 1)
        person = make_person(pid=1, prescriptions=[])
        pe_map = _build_pe_map([person], [anchor])

        for anchor_data in pe_map.values():
            for pe_data in anchor_data.values():
                for cohort_data in pe_data.values():
                    for gender_ids in cohort_data.values():
                        assert isinstance(gender_ids, list)

    def test_all_anchor_dates_present_in_output(self):
        anchors = [date(2021, 6, 1), date(2021, 6, 2), date(2021, 6, 3)]
        person = make_person(pid=1, prescriptions=[])
        pe_map = _build_pe_map([person], anchors)

        for a in anchors:
            assert a in pe_map

    def test_no_duplicate_person_ids_in_same_slot(self):
        """A person should appear at most once per (anchor, pe_cat, cohort, gender)."""
        anchors = [date(2021, 6, 1)]
        person = make_person(pid=1, prescriptions=[])
        pe_map = _build_pe_map([person], anchors)

        for anchor_data in pe_map.values():
            for pe_data in anchor_data.values():
                for cohort_data in pe_data.values():
                    for gender_ids in cohort_data.values():
                        assert len(gender_ids) == len(set(gender_ids))


# ===================================================================
# Consistency check: PE window computer vs sum_before_date_pe
# ===================================================================


class TestConsistencyWithSumBefore:
    """These tests document the boundary discrepancy between the PE window
    computer (closed interval) and sum_before_date_pe_for_person (open interval).

    Ideally these should produce the same PE classification for matching
    to be consistent. Tests are written based on THEORETICAL expectations
    and may expose implementation inconsistencies.
    """

    def test_prescription_on_anchor_date_discrepancy(self):
        """A prescription exactly on the anchor date:
        - PE window computer: INCLUDES it (pr.date <= anchor)
        - sum_before: EXCLUDES it (pr.date < ddate is False)

        This test documents the discrepancy: the PE window computer places the
        person in a non-zero bucket, but sum_before would compute 0 PE.
        """
        from common.matching_simulation.utils import sum_before_date_pe_for_person

        anchor = date(2021, 6, 1)
        pr_date = date(2021, 6, 1)
        person = make_person(
            pid=1, prescriptions=[make_prescription(pr_date, pe=100.0)]
        )

        # sum_before excludes the prescription on the exact date
        pe_from_sum = sum_before_date_pe_for_person(person, pr_date)
        assert pe_from_sum == 0.0, "sum_before excludes prescription on exact date"

        # But the PE window computer includes it
        pe_map = _build_pe_map([person], [anchor])
        cat_100 = from_prednison_equiv(100.0)
        person_has_pe_in_map = _person_in_map(
            pe_map,
            anchor,
            cat_100,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        )
        assert person_has_pe_in_map, (
            "PE window computer includes prescription on anchor date, "
            "creating inconsistency with sum_before"
        )

    def test_prescription_exactly_365_days_before_discrepancy(self):
        """A prescription exactly 365 days before the anchor:
        - PE window computer: INCLUDES it (anchor == pr.date + 365)
        - sum_before: EXCLUDES it (pr.date > ddate - 365 is False)
        """
        from common.matching_simulation.utils import sum_before_date_pe_for_person

        anchor = date(2021, 6, 1)
        pr_date = anchor - timedelta(days=365)
        person = make_person(
            pid=1, prescriptions=[make_prescription(pr_date, pe=100.0)]
        )

        pe_from_sum = sum_before_date_pe_for_person(person, anchor)
        assert pe_from_sum == 0.0, (
            "sum_before excludes prescription exactly 365 days before"
        )

        pe_map = _build_pe_map([person], [anchor])
        cat_100 = from_prednison_equiv(100.0)
        assert _person_in_map(
            pe_map,
            anchor,
            cat_100,
            AgeCohort._30_49,
            Gender.MALE,
            1,
        ), (
            "PE window computer includes prescription 365 days before, "
            "creating inconsistency with sum_before"
        )
