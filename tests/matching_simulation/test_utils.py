"""Tests for common.matching_simulation.utils."""

from datetime import date, datetime, timedelta

import pytest

from common.matching_simulation.utils import (
    PREDNISON_EQUIV_CATEGORY,
    PE_GROUP_NAMES,
    AgeCohort,
    MatchingAnalysisConfig,
    from_prednison_equiv,
    has_prescriptions_before_date,
    is_injection,
    is_zero_pe_group,
    sum_after_date_pe_for_person,
    sum_before_date_pe_for_person,
)

from .conftest import make_person, make_prescription


# ===================================================================
# MatchingAnalysisConfig
# ===================================================================


class TestMatchingAnalysisConfig:
    def test_start_and_end_date_no_offset(self):
        cfg = MatchingAnalysisConfig(
            pojistovna="test",
            zacatek_pojisteni=date(2020, 1, 1),
            konec_pojisteni=date(2023, 1, 1),
            year_offset=0,
        )
        assert cfg.start_date == datetime(2021, 1, 1)
        assert cfg.end_date == datetime(2022, 2, 28)

    def test_start_and_end_date_with_offset(self):
        cfg = MatchingAnalysisConfig(
            pojistovna="test",
            zacatek_pojisteni=date(2020, 1, 1),
            konec_pojisteni=date(2023, 1, 1),
            year_offset=1,
        )
        expected_start = datetime(2021, 1, 1) - timedelta(days=365)
        expected_end = datetime(2022, 2, 28) - timedelta(days=365)
        assert cfg.start_date == expected_start
        assert cfg.end_date == expected_end

    def test_year_for_age_calculation(self):
        cfg = MatchingAnalysisConfig(
            pojistovna="test",
            zacatek_pojisteni=date(2020, 1, 1),
            konec_pojisteni=date(2023, 1, 1),
            year_offset=2,
        )
        assert cfg.year_for_age_calculation == 2019

    def test_anchor_dates_are_contiguous(self, default_config):
        anchors = default_config.anchor_dates
        assert len(anchors) > 0
        assert anchors[0] == date(2021, 1, 1)
        assert anchors[-1] == date(2022, 2, 28)
        for i in range(1, len(anchors)):
            assert anchors[i] - anchors[i - 1] == timedelta(days=1)

    def test_maximum_aggregation_days(self, default_config):
        assert default_config.maximum_aggregation_days == len(
            default_config.anchor_dates
        )


# ===================================================================
# AgeCohortCalculator
# ===================================================================


class TestAgeCohortCalculator:
    """Age cohort boundaries: 12-15, 16-22, 23-29, 30-39, 40-49, 50-59, else IRRELEVANT."""

    @pytest.mark.parametrize(
        "born_year, expected",
        [
            # year_for_age_calculation=2021
            (2021 - 11, AgeCohort.IRRELEVANT),  # age 11 → too young
            (2021 - 12, AgeCohort._12_15),  # age 12 → lower bound
            (2021 - 15, AgeCohort._12_15),  # age 15 → upper bound
            (2021 - 16, AgeCohort._16_22),  # age 16 → lower bound
            (2021 - 22, AgeCohort._16_22),  # age 22 → upper bound
            (2021 - 23, AgeCohort._23_29),  # age 23 → lower bound
            (2021 - 29, AgeCohort._23_29),  # age 29 → upper bound
            (2021 - 30, AgeCohort._30_39),  # age 30 → lower bound
            (2021 - 39, AgeCohort._30_39),  # age 39 → upper bound
            (2021 - 40, AgeCohort._40_49),  # age 40 → lower bound
            (2021 - 49, AgeCohort._40_49),  # age 49 → upper bound
            (2021 - 50, AgeCohort._50_59),  # age 50 → lower bound
            (2021 - 59, AgeCohort._50_59),  # age 59 → upper bound
            (2021 - 60, AgeCohort.IRRELEVANT),  # age 60 → too old
        ],
    )
    def test_age_boundaries(self, age_calculator, born_year, expected):
        person = make_person(born_year=born_year)
        assert age_calculator.calculate_age_cohort(person) == expected


# ===================================================================
# is_injection
# ===================================================================


class TestIsInjection:
    def test_injection_prescription(self):
        pr = make_prescription(date(2021, 5, 1), injection=True)
        assert is_injection(pr) is True

    def test_non_injection_prescription(self):
        pr = make_prescription(date(2021, 5, 1), injection=False)
        assert is_injection(pr) is False

    def test_none_lekova_forma_zkr(self):
        pr = make_prescription(date(2021, 5, 1))
        pr.lekova_forma_zkr = None
        assert is_injection(pr) is False


# ===================================================================
# from_prednison_equiv
# ===================================================================


class TestFromPrednisonEquiv:
    def test_zero_returns_zero_pe(self):
        assert from_prednison_equiv(0) == PREDNISON_EQUIV_CATEGORY.ZERO_PE

    def test_small_positive_value(self):
        result = from_prednison_equiv(10.0)
        assert result == PREDNISON_EQUIV_CATEGORY["BETWEEN_0_AND_25"]

    def test_exact_boundary_25(self):
        """25.0 should fall in BETWEEN_25_AND_50, not BETWEEN_0_AND_25."""
        result = from_prednison_equiv(25.0)
        assert result == PREDNISON_EQUIV_CATEGORY["BETWEEN_25_AND_50"]

    def test_exact_boundary_50(self):
        result = from_prednison_equiv(50.0)
        assert result == PREDNISON_EQUIV_CATEGORY["BETWEEN_50_AND_75"]

    def test_just_below_boundary(self):
        result = from_prednison_equiv(24.9)
        assert result == PREDNISON_EQUIV_CATEGORY["BETWEEN_0_AND_25"]

    def test_value_at_5000(self):
        result = from_prednison_equiv(5000.0)
        assert result == PREDNISON_EQUIV_CATEGORY["MORE_THAN_5000"]

    def test_value_above_5000(self):
        result = from_prednison_equiv(9999.0)
        assert result == PREDNISON_EQUIV_CATEGORY["MORE_THAN_5000"]

    def test_value_just_below_5000(self):
        result = from_prednison_equiv(4999.0)
        assert result == PREDNISON_EQUIV_CATEGORY["BETWEEN_4975_AND_5000"]


# ===================================================================
# is_zero_pe_group
# ===================================================================


class TestIsZeroPeGroup:
    @pytest.mark.parametrize(
        "group, expected",
        [
            (PE_GROUP_NAMES.ZERO_PE, True),
            (PE_GROUP_NAMES.NEVER_PRESCRIBED, True),
            (PE_GROUP_NAMES.ZERO_PE_SUSPECTIBLE, True),
            (PE_GROUP_NAMES.ONE_TO_FIVE_HUNDRED_PE, False),
            (PE_GROUP_NAMES.FIVE_HUNDRED_TO_FIVE_THOUSAND_PE, False),
        ],
    )
    def test_classification(self, group, expected):
        assert is_zero_pe_group(group) is expected


# ===================================================================
# has_prescriptions_before_date
# ===================================================================


class TestHasPrescriptionsBeforeDate:
    def test_prescription_before(self):
        person = make_person(prescriptions=[make_prescription(date(2021, 3, 1))])
        assert has_prescriptions_before_date(person, date(2021, 6, 1)) is True

    def test_prescription_after(self):
        person = make_person(prescriptions=[make_prescription(date(2021, 9, 1))])
        assert has_prescriptions_before_date(person, date(2021, 6, 1)) is False

    def test_prescription_on_exact_date(self):
        """Prescription on the exact anchor date should NOT count as 'before'."""
        person = make_person(prescriptions=[make_prescription(date(2021, 6, 1))])
        assert has_prescriptions_before_date(person, date(2021, 6, 1)) is False

    def test_no_prescriptions(self):
        person = make_person(prescriptions=[])
        assert has_prescriptions_before_date(person, date(2021, 6, 1)) is False


# ===================================================================
# sum_before_date_pe_for_person  /  sum_after_date_pe_for_person
# ===================================================================


class TestSumBeforeDatePe:
    """Tests for sum_before_date_pe_for_person.

    Window: (ddate - 365, ddate) — both boundaries exclusive.
    """

    def test_basic_sum(self):
        person = make_person(
            prescriptions=[
                make_prescription(date(2021, 3, 1), pe=100.0),
                make_prescription(date(2021, 4, 1), pe=50.0),
            ]
        )
        result = sum_before_date_pe_for_person(person, date(2021, 6, 1))
        assert result == 150.0

    def test_excludes_injections(self):
        person = make_person(
            prescriptions=[
                make_prescription(date(2021, 3, 1), pe=100.0),
                make_prescription(date(2021, 4, 1), pe=200.0, injection=True),
            ]
        )
        result = sum_before_date_pe_for_person(person, date(2021, 6, 1))
        assert result == 100.0

    def test_excludes_prescription_on_exact_date(self):
        """Prescription exactly on ddate is excluded (open upper bound)."""
        person = make_person(
            prescriptions=[make_prescription(date(2021, 6, 1), pe=100.0)]
        )
        result = sum_before_date_pe_for_person(person, date(2021, 6, 1))
        assert result == 0.0

    def test_excludes_prescription_exactly_365_days_before(self):
        """Prescription exactly 365 days before ddate is excluded (open lower bound)."""
        anchor = date(2021, 6, 1)
        pr_date = anchor - timedelta(days=365)
        person = make_person(prescriptions=[make_prescription(pr_date, pe=100.0)])
        result = sum_before_date_pe_for_person(person, anchor)
        assert result == 0.0

    def test_includes_prescription_364_days_before(self):
        """Prescription 364 days before ddate is inside the window."""
        anchor = date(2021, 6, 1)
        pr_date = anchor - timedelta(days=364)
        person = make_person(prescriptions=[make_prescription(pr_date, pe=100.0)])
        result = sum_before_date_pe_for_person(person, anchor)
        assert result == 100.0

    def test_excludes_prescriptions_outside_window(self):
        anchor = date(2021, 6, 1)
        person = make_person(
            prescriptions=[
                make_prescription(anchor - timedelta(days=400), pe=999.0),
                make_prescription(anchor + timedelta(days=1), pe=888.0),
            ]
        )
        result = sum_before_date_pe_for_person(person, anchor)
        assert result == 0.0


class TestSumAfterDatePe:
    """Tests for sum_after_date_pe_for_person.

    Window: (ddate, ddate + 365) — both boundaries exclusive.
    """

    def test_basic_sum(self):
        person = make_person(
            prescriptions=[
                make_prescription(date(2021, 7, 1), pe=80.0),
                make_prescription(date(2021, 8, 1), pe=20.0),
            ]
        )
        result = sum_after_date_pe_for_person(person, date(2021, 6, 1))
        assert result == 100.0

    def test_excludes_injections(self):
        person = make_person(
            prescriptions=[
                make_prescription(date(2021, 7, 1), pe=80.0),
                make_prescription(date(2021, 8, 1), pe=200.0, injection=True),
            ]
        )
        result = sum_after_date_pe_for_person(person, date(2021, 6, 1))
        assert result == 80.0

    def test_excludes_prescription_on_exact_date(self):
        """Prescription exactly on ddate is excluded (open lower bound)."""
        person = make_person(
            prescriptions=[make_prescription(date(2021, 6, 1), pe=100.0)]
        )
        result = sum_after_date_pe_for_person(person, date(2021, 6, 1))
        assert result == 0.0

    def test_excludes_prescription_exactly_365_days_after(self):
        """Prescription exactly 365 days after ddate is excluded (open upper bound)."""
        anchor = date(2021, 6, 1)
        pr_date = anchor + timedelta(days=365)
        person = make_person(prescriptions=[make_prescription(pr_date, pe=100.0)])
        result = sum_after_date_pe_for_person(person, anchor)
        assert result == 0.0

    def test_includes_prescription_364_days_after(self):
        anchor = date(2021, 6, 1)
        pr_date = anchor + timedelta(days=364)
        person = make_person(prescriptions=[make_prescription(pr_date, pe=100.0)])
        result = sum_after_date_pe_for_person(person, anchor)
        assert result == 100.0
