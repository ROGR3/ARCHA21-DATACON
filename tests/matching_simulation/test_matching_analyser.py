"""Tests for MatchingAnalyser.

These tests are written from theoretical first principles of the matching
analysis methodology:

- Zero-PE groups: treatment effect = vax_after / novax_after (ratio, baseline=1)
- Non-zero-PE groups: treatment effect = (vax_after/vax_before) - (novax_after/novax_before)
  (difference-in-differences, baseline=0)

Tests focus on:
1. Effect computation logic (the core formulas)
2. Date aggregation
3. Matching person lookup
4. Statistical aggregation
"""

import random
from collections import defaultdict
from datetime import date, datetime, timedelta

import numpy as np
import pytest

from common.constants.objects import Gender
from common.matching_simulation.matching_analyser import MatchingAnalyser
from common.matching_simulation.utils import (
    PE_GROUP_NAMES,
    PREDNISON_EQUIV_CATEGORY,
    AgeCohort,
    AgeCohortCalculator,
    MatchingAnalysisConfig,
    PeMap,
    from_prednison_equiv,
)

from .conftest import make_person, make_prescription, make_vaccine


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


def _make_analyser(config: MatchingAnalysisConfig | None = None) -> MatchingAnalyser:
    config = config or _make_config()
    calc = AgeCohortCalculator(config)
    return MatchingAnalyser(config, calc)


# Wrappers for name-mangled private methods so type: ignore is in one place.
def _compute_effect_values(analyser: MatchingAnalyser, *args, **kwargs):  # noqa: N802
    fn = getattr(analyser, "_MatchingAnalyser__compute_effect_values")
    return fn(*args, **kwargs)


def _aggregate_date(analyser: MatchingAnalyser, *args, **kwargs):
    fn = getattr(analyser, "_MatchingAnalyser__aggregate_date")
    return fn(*args, **kwargs)


def _find_matching_person(analyser: MatchingAnalyser, *args, **kwargs):
    fn = getattr(analyser, "_MatchingAnalyser__find_matching_person")
    return fn(*args, **kwargs)


def _compute_statistics(analyser: MatchingAnalyser, *args, **kwargs):
    fn = getattr(analyser, "_MatchingAnalyser__compute_statistics")
    return fn(*args, **kwargs)


# ===================================================================
# Effect computation — zero PE groups
# ===================================================================


class TestEffectComputationZeroPE:
    """For zero-PE groups the formula is: effect = vax_after / novax_after.

    This is a simple ratio — when vaccination has no effect on PE usage,
    the ratio should be ~1.0.
    """

    def test_equal_after_pe_gives_ratio_one(self):
        """When vax and novax have the same after-PE, effect = 1.0."""
        analyser = _make_analyser()
        vax_before = {AgeCohort._30_49: {datetime(2021, 6, 1): 0.0}}
        vax_after = {AgeCohort._30_49: {datetime(2021, 6, 1): 100.0}}
        novax_before = {AgeCohort._30_49: {datetime(2021, 6, 1): 0.0}}
        novax_after = {AgeCohort._30_49: {datetime(2021, 6, 1): 100.0}}

        result = _compute_effect_values(
            analyser,
            vax_before,
            vax_after,
            novax_before,
            novax_after,
            PE_GROUP_NAMES.ZERO_PE,
        )
        assert result[AgeCohort._30_49][datetime(2021, 6, 1)] == pytest.approx(1.0)

    def test_higher_vax_after_gives_ratio_above_one(self):
        """More PE after vax than after novax -> ratio > 1."""
        analyser = _make_analyser()
        vax_after = {AgeCohort._30_49: {datetime(2021, 6, 1): 200.0}}
        novax_after = {AgeCohort._30_49: {datetime(2021, 6, 1): 100.0}}

        result = _compute_effect_values(
            analyser,
            defaultdict(dict),
            vax_after,
            defaultdict(dict),
            novax_after,
            PE_GROUP_NAMES.ZERO_PE,
        )
        assert result[AgeCohort._30_49][datetime(2021, 6, 1)] == pytest.approx(2.0)

    def test_zero_novax_after_skips(self):
        """Division by zero: when novax_after=0, the date should be skipped."""
        analyser = _make_analyser()
        vax_after = {AgeCohort._30_49: {datetime(2021, 6, 1): 100.0}}
        novax_after = {AgeCohort._30_49: {datetime(2021, 6, 1): 0.0}}

        result = _compute_effect_values(
            analyser,
            defaultdict(dict),
            vax_after,
            defaultdict(dict),
            novax_after,
            PE_GROUP_NAMES.ZERO_PE,
        )
        assert datetime(2021, 6, 1) not in result.get(AgeCohort._30_49, {})

    def test_never_prescribed_uses_zero_pe_formula(self):
        """NEVER_PRESCRIBED is a zero-PE group -> same ratio formula."""
        analyser = _make_analyser()
        vax_after = {AgeCohort._30_49: {datetime(2021, 6, 1): 150.0}}
        novax_after = {AgeCohort._30_49: {datetime(2021, 6, 1): 100.0}}

        result = _compute_effect_values(
            analyser,
            defaultdict(dict),
            vax_after,
            defaultdict(dict),
            novax_after,
            PE_GROUP_NAMES.NEVER_PRESCRIBED,
        )
        assert result[AgeCohort._30_49][datetime(2021, 6, 1)] == pytest.approx(1.5)


# ===================================================================
# Effect computation — non-zero PE groups (diff-in-diff)
# ===================================================================


class TestEffectComputationNonZeroPE:
    """For non-zero PE groups the formula is:
        effect = (vax_after / vax_before) - (novax_after / novax_before)

    This is a difference-in-differences approach. When there's no treatment
    effect, the within-group before/after ratios should be equal -> effect = 0.
    """

    def test_no_treatment_effect_gives_zero(self):
        """When both groups change by the same ratio, DID = 0."""
        analyser = _make_analyser()
        dt = datetime(2021, 6, 1)
        vax_before = {AgeCohort._30_49: {dt: 200.0}}
        vax_after = {AgeCohort._30_49: {dt: 100.0}}  # ratio = 0.5
        novax_before = {AgeCohort._30_49: {dt: 400.0}}
        novax_after = {AgeCohort._30_49: {dt: 200.0}}  # ratio = 0.5

        result = _compute_effect_values(
            analyser,
            vax_before,
            vax_after,
            novax_before,
            novax_after,
            PE_GROUP_NAMES.ONE_TO_FIVE_HUNDRED_PE,
        )
        assert result[AgeCohort._30_49][dt] == pytest.approx(0.0)

    def test_positive_treatment_effect(self):
        """vax increases more than novax -> positive DID."""
        analyser = _make_analyser()
        dt = datetime(2021, 6, 1)
        vax_before = {AgeCohort._30_49: {dt: 100.0}}
        vax_after = {AgeCohort._30_49: {dt: 200.0}}  # ratio = 2.0
        novax_before = {AgeCohort._30_49: {dt: 100.0}}
        novax_after = {AgeCohort._30_49: {dt: 100.0}}  # ratio = 1.0

        result = _compute_effect_values(
            analyser,
            vax_before,
            vax_after,
            novax_before,
            novax_after,
            PE_GROUP_NAMES.ONE_TO_FIVE_HUNDRED_PE,
        )
        # DID = 2.0 - 1.0 = 1.0
        assert result[AgeCohort._30_49][dt] == pytest.approx(1.0)

    def test_negative_treatment_effect(self):
        """vax decreases more than novax -> negative DID."""
        analyser = _make_analyser()
        dt = datetime(2021, 6, 1)
        vax_before = {AgeCohort._30_49: {dt: 200.0}}
        vax_after = {AgeCohort._30_49: {dt: 100.0}}  # ratio = 0.5
        novax_before = {AgeCohort._30_49: {dt: 200.0}}
        novax_after = {AgeCohort._30_49: {dt: 200.0}}  # ratio = 1.0

        result = _compute_effect_values(
            analyser,
            vax_before,
            vax_after,
            novax_before,
            novax_after,
            PE_GROUP_NAMES.FIVE_HUNDRED_TO_FIVE_THOUSAND_PE,
        )
        # DID = 0.5 - 1.0 = -0.5
        assert result[AgeCohort._30_49][dt] == pytest.approx(-0.5)

    def test_zero_vax_before_skips(self):
        """Division by zero: vax_before=0 -> date skipped."""
        analyser = _make_analyser()
        dt = datetime(2021, 6, 1)
        vax_before = {AgeCohort._30_49: {dt: 0.0}}
        vax_after = {AgeCohort._30_49: {dt: 100.0}}
        novax_before = {AgeCohort._30_49: {dt: 100.0}}
        novax_after = {AgeCohort._30_49: {dt: 100.0}}

        result = _compute_effect_values(
            analyser,
            vax_before,
            vax_after,
            novax_before,
            novax_after,
            PE_GROUP_NAMES.ONE_TO_FIVE_HUNDRED_PE,
        )
        assert dt not in result.get(AgeCohort._30_49, {})

    def test_zero_novax_before_skips(self):
        """Division by zero: novax_before=0 -> date skipped."""
        analyser = _make_analyser()
        dt = datetime(2021, 6, 1)
        vax_before = {AgeCohort._30_49: {dt: 100.0}}
        vax_after = {AgeCohort._30_49: {dt: 100.0}}
        novax_before = {AgeCohort._30_49: {dt: 0.0}}
        novax_after = {AgeCohort._30_49: {dt: 100.0}}

        result = _compute_effect_values(
            analyser,
            vax_before,
            vax_after,
            novax_before,
            novax_after,
            PE_GROUP_NAMES.ONE_TO_FIVE_HUNDRED_PE,
        )
        assert dt not in result.get(AgeCohort._30_49, {})


# ===================================================================
# Effect computation — multiple cohorts
# ===================================================================


class TestEffectMultipleCohorts:
    def test_each_cohort_computed_independently(self):
        analyser = _make_analyser()
        dt = datetime(2021, 6, 1)

        vax_after = {
            AgeCohort._16_29: {dt: 100.0},
            AgeCohort._30_49: {dt: 300.0},
        }
        novax_after = {
            AgeCohort._16_29: {dt: 50.0},
            AgeCohort._30_49: {dt: 100.0},
        }

        result = _compute_effect_values(
            analyser,
            defaultdict(dict),
            vax_after,
            defaultdict(dict),
            novax_after,
            PE_GROUP_NAMES.ZERO_PE,
        )
        assert result[AgeCohort._16_29][dt] == pytest.approx(2.0)
        assert result[AgeCohort._30_49][dt] == pytest.approx(3.0)


# ===================================================================
# Date aggregation
# ===================================================================


class TestDateAggregation:
    """__aggregate_date bins dates into windows of `window_days` width."""

    def test_window_1_returns_same_date(self):
        analyser = _make_analyser()
        dt = date(2021, 3, 15)
        result = _aggregate_date(analyser, dt, 1)
        assert result == datetime(2021, 3, 15)

    def test_window_7_bins_correctly(self):
        """Dates within the same 7-day bin from epoch should map to the same date."""
        config = _make_config()
        analyser = _make_analyser(config)

        epoch = config.start_date  # 2021-01-01

        # Day 0, 1, ..., 6 -> all in the same bin (starts at day 0)
        for i in range(7):
            dt = (epoch + timedelta(days=i)).date()
            result = _aggregate_date(analyser, dt, 7)
            assert result == epoch, f"Day {i} should map to epoch"

        # Day 7 -> next bin
        dt7 = (epoch + timedelta(days=7)).date()
        result = _aggregate_date(analyser, dt7, 7)
        assert result == epoch + timedelta(days=7)

    def test_window_30_bins(self):
        config = _make_config()
        analyser = _make_analyser(config)
        epoch = config.start_date

        dt_day_29 = (epoch + timedelta(days=29)).date()
        dt_day_30 = (epoch + timedelta(days=30)).date()

        assert _aggregate_date(analyser, dt_day_29, 30) == epoch
        assert _aggregate_date(analyser, dt_day_30, 30) == epoch + timedelta(days=30)


# ===================================================================
# Matching person lookup
# ===================================================================


class TestFindMatchingPerson:
    """__find_matching_person should pick a random novax person from the
    correct (date, PE bucket, age cohort, gender) slot."""

    def test_returns_person_from_correct_slot(self):
        analyser = _make_analyser()

        vax_person = make_person(
            pid=100,
            gender=Gender.MALE,
            born_year=1985,
            vaccines=[make_vaccine(date(2021, 6, 1))],
        )

        pe_range = PREDNISON_EQUIV_CATEGORY.ZERO_PE
        novax_ids = [201, 202, 203]
        pe_map: PeMap = {
            date(2021, 6, 1): {
                pe_range: {
                    AgeCohort._30_49: {
                        Gender.MALE: novax_ids,
                    }
                }
            }
        }

        random.seed(42)
        result = _find_matching_person(
            analyser,
            vax_person,
            pe_range,
            date(2021, 6, 1),
            pe_map,
        )
        assert result in novax_ids

    def test_raises_on_missing_slot(self):
        """When no matching novax people exist, a KeyError should propagate."""
        analyser = _make_analyser()

        vax_person = make_person(
            pid=100,
            gender=Gender.MALE,
            born_year=1985,
            vaccines=[make_vaccine(date(2021, 6, 1))],
        )

        pe_map: PeMap = {date(2021, 6, 1): {}}

        with pytest.raises(KeyError):
            _find_matching_person(
                analyser,
                vax_person,
                PREDNISON_EQUIV_CATEGORY.ZERO_PE,
                date(2021, 6, 1),
                pe_map,
            )


# ===================================================================
# Statistics computation
# ===================================================================


class TestComputeStatistics:
    """__compute_statistics should produce median, IQR, and 95% CI."""

    def test_deterministic_values(self):
        analyser = _make_analyser()
        dt = datetime(2021, 6, 1)

        effects = {
            AgeCohort._30_49: {
                dt: [1.0, 2.0, 3.0, 4.0, 5.0],
            }
        }

        median_map, iqr_map, ci_map = _compute_statistics(analyser, effects)

        assert median_map[AgeCohort._30_49][dt] == pytest.approx(3.0)

        q1, q3 = iqr_map[AgeCohort._30_49][dt]
        assert q1 == pytest.approx(np.percentile([1, 2, 3, 4, 5], 25))
        assert q3 == pytest.approx(np.percentile([1, 2, 3, 4, 5], 75))

        ci_low, ci_high = ci_map[AgeCohort._30_49][dt]
        assert ci_low == pytest.approx(np.percentile([1, 2, 3, 4, 5], 2.5))
        assert ci_high == pytest.approx(np.percentile([1, 2, 3, 4, 5], 97.5))

    def test_single_value(self):
        """With a single observation, median = that value, CI collapses."""
        analyser = _make_analyser()
        dt = datetime(2021, 6, 1)

        effects = {AgeCohort._30_49: {dt: [42.0]}}
        median_map, iqr_map, ci_map = _compute_statistics(analyser, effects)

        assert median_map[AgeCohort._30_49][dt] == pytest.approx(42.0)

    def test_all_equal_values(self):
        analyser = _make_analyser()
        dt = datetime(2021, 6, 1)

        effects = {AgeCohort._30_49: {dt: [5.0] * 100}}
        median_map, iqr_map, ci_map = _compute_statistics(analyser, effects)

        assert median_map[AgeCohort._30_49][dt] == pytest.approx(5.0)
        q1, q3 = iqr_map[AgeCohort._30_49][dt]
        assert q1 == pytest.approx(5.0)
        assert q3 == pytest.approx(5.0)


# ===================================================================
# Vaccination dates distribution
# ===================================================================


class TestVaxDatesDistribution:
    def test_counts_by_cohort_and_date(self):
        analyser = _make_analyser()

        p1 = make_person(
            pid=1,
            born_year=1985,
            vaccines=[make_vaccine(date(2021, 3, 1))],
        )
        p2 = make_person(
            pid=2,
            born_year=1985,
            vaccines=[make_vaccine(date(2021, 3, 1))],
        )
        p3 = make_person(
            pid=3,
            born_year=2000,
            vaccines=[make_vaccine(date(2021, 3, 1))],
        )

        dist = analyser.get_vax_dates_distribution([p1, p2, p3], aggregation_days=1)

        # aggregate_date converts date -> datetime
        assert dist[AgeCohort._30_49][datetime(2021, 3, 1)] == 2.0
        assert dist[AgeCohort._16_29][datetime(2021, 3, 1)] == 1.0


# ===================================================================
# Integration: run_matching_analysis with controlled matching
# ===================================================================


class TestRunMatchingAnalysisIntegration:
    """Lightweight integration test for the full pipeline."""

    def test_zero_pe_group_full_pipeline(self):
        """Run matching analysis for a zero-PE group with a single
        vax person and a single matching novax person."""
        config = _make_config()
        analyser = _make_analyser(config)

        vax_date = date(2021, 6, 1)
        vax_person = make_person(
            pid=1,
            born_year=1985,
            gender=Gender.MALE,
            vaccines=[make_vaccine(vax_date)],
            prescriptions=[
                make_prescription(date(2021, 8, 1), pe=100.0),
            ],
        )

        novax_person = make_person(
            pid=2,
            born_year=1985,
            gender=Gender.MALE,
            prescriptions=[
                make_prescription(date(2021, 8, 1), pe=50.0),
            ],
        )

        person_map = {1: vax_person, 2: novax_person}

        pe_map: PeMap = {
            vax_date: {
                PREDNISON_EQUIV_CATEGORY.ZERO_PE: {
                    AgeCohort._30_49: {
                        Gender.MALE: [2],
                    }
                }
            }
        }

        median_map, iqr_map, ci_map = analyser.run_matching_analysis(
            people=[vax_person],
            person_map=person_map,
            pe_map=pe_map,
            aggregation_days=config.maximum_aggregation_days,
            group_name=PE_GROUP_NAMES.ZERO_PE,
            num_runs=5,
        )

        # vax_after=100, novax_after=50 -> effect = 100/50 = 2.0
        # All runs produce the same result since there's only one match
        assert AgeCohort._30_49 in median_map
        for dt, val in median_map[AgeCohort._30_49].items():
            assert val == pytest.approx(2.0)

    def test_nonzero_pe_group_full_pipeline(self):
        """DID calculation with controlled before/after values."""
        config = _make_config()
        analyser = _make_analyser(config)

        vax_date = date(2021, 6, 1)

        # Vax person: before=100, after=200 -> ratio = 2.0
        vax_person = make_person(
            pid=1,
            born_year=1985,
            gender=Gender.MALE,
            vaccines=[make_vaccine(vax_date)],
            prescriptions=[
                make_prescription(date(2021, 3, 1), pe=100.0),  # before
                make_prescription(date(2021, 8, 1), pe=200.0),  # after
            ],
        )

        # Novax person: before=100, after=100 -> ratio = 1.0
        novax_person = make_person(
            pid=2,
            born_year=1985,
            gender=Gender.MALE,
            prescriptions=[
                make_prescription(date(2021, 3, 1), pe=100.0),
                make_prescription(date(2021, 8, 1), pe=100.0),
            ],
        )

        person_map = {1: vax_person, 2: novax_person}

        pe_range = from_prednison_equiv(100.0)
        pe_map: PeMap = {
            vax_date: {
                pe_range: {
                    AgeCohort._30_49: {
                        Gender.MALE: [2],
                    }
                }
            }
        }

        median_map, iqr_map, ci_map = analyser.run_matching_analysis(
            people=[vax_person],
            person_map=person_map,
            pe_map=pe_map,
            aggregation_days=config.maximum_aggregation_days,
            group_name=PE_GROUP_NAMES.ONE_TO_FIVE_HUNDRED_PE,
            num_runs=5,
        )

        # DID = (200/100) - (100/100) = 2.0 - 1.0 = 1.0
        assert AgeCohort._30_49 in median_map
        for dt, val in median_map[AgeCohort._30_49].items():
            assert val == pytest.approx(1.0)

    def test_person_above_5000_pe_is_skipped(self):
        """People with vax_before PE > 5000 should be excluded."""
        config = _make_config()
        analyser = _make_analyser(config)

        vax_date = date(2021, 6, 1)
        vax_person = make_person(
            pid=1,
            born_year=1985,
            gender=Gender.MALE,
            vaccines=[make_vaccine(vax_date)],
            prescriptions=[
                make_prescription(date(2021, 3, 1), pe=5001.0),
            ],
        )
        person_map = {1: vax_person}
        pe_map: PeMap = {}

        median_map, _, _ = analyser.run_matching_analysis(
            people=[vax_person],
            person_map=person_map,
            pe_map=pe_map,
            aggregation_days=config.maximum_aggregation_days,
            group_name=PE_GROUP_NAMES.ONE_TO_FIVE_HUNDRED_PE,
            num_runs=2,
        )
        # Person was skipped -> no results
        assert len(median_map) == 0
