#!/usr/bin/env python3
"""Compare Colorado SNAP RuleSpec output against PolicyEngine enhanced CPS.

The script projects PolicyEngine eCPS SPM-unit records into the current
Colorado SNAP composition input surface, including related member facts, runs
the Axiom Rules engine once over those projected records, and compares regular
monthly SNAP allotments against PolicyEngine's normal allotment. It uses these
targets because the enhanced CPS records do not include application dates for
initial-month proration, and PolicyEngine's top-level ``snap`` microsimulation
value includes take-up adjustments.

For oracle parity, use ``--utility-projection policyengine-type``. That maps
PolicyEngine's own eCPS utility-allowance type into the closest Colorado
utility facts. The default ``raw-expenses`` mode projects itemized eCPS utility
expenses directly and is useful for diagnosing data-projection gaps.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
import tempfile
from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
AXIOM_ROOT = REPO_ROOT.parent
DEFAULT_PROGRAM = (
    REPO_ROOT / "policies" / "cdhs" / "snap" / "fy-2026-benefit-calculation.yaml"
)
DEFAULT_TEST_TEMPLATE = DEFAULT_PROGRAM.with_name(f"{DEFAULT_PROGRAM.stem}.test.yaml")
DEFAULT_AXIOM_BINARY = AXIOM_ROOT / "axiom-rules" / "target" / "debug" / "axiom-rules"
PE_COMPARED_OUTPUT = "snap_normal_allotment"
AXIOM_OUTPUT_ID_BY_LABEL = {
    "snap_regular_month_allotment": "us:statutes/7/2017/a#snap_regular_month_allotment",
    "snap_eligible": (
        "us-co:policies/cdhs/snap/fy-2026-benefit-calculation#snap_eligible"
    ),
    "snap_gross_monthly_income": (
        "us:regulations/7-cfr/273/10#snap_gross_monthly_income"
    ),
    "snap_net_income": "us:statutes/7/2014/e/6/A#snap_net_income",
    "snap_maximum_allotment": (
        "us:policies/usda/snap/fy-2026-cola/maximum-allotments#snap_maximum_allotment"
    ),
    "snap_standard_utility_allowance": (
        "us-co:regulations/10-ccr-2506-1/4.407.31#snap_standard_utility_allowance"
    ),
    "snap_limited_utility_allowance": (
        "us-co:regulations/10-ccr-2506-1/4.407.31#snap_limited_utility_allowance"
    ),
    "snap_one_utility_allowance": (
        "us-co:regulations/10-ccr-2506-1/4.407.31#snap_one_utility_allowance"
    ),
    "snap_individual_utility_allowance": (
        "us-co:regulations/10-ccr-2506-1/4.407.31#snap_individual_utility_allowance"
    ),
    "snap_excess_shelter_deduction": (
        "us:regulations/7-cfr/273/10#snap_excess_shelter_deduction"
    ),
}
COMPARED_AXIOM_OUTPUT = "snap_regular_month_allotment"
AXIOM_OUTPUTS = list(AXIOM_OUTPUT_ID_BY_LABEL.values())
AXIOM_RELATION_ID_BY_LABEL = {
    "member_of_household": "us:statutes/7/2012/j#relation.member_of_household",
}
AXIOM_MEMBER_INPUT_ID_BY_LABEL = {
    "snap_member_is_elderly_or_disabled": (
        "us:statutes/7/2012/j#input.snap_member_is_elderly_or_disabled"
    ),
    "member_is_us_citizen": (
        "us:regulations/7-cfr/273/4#input.member_is_us_citizen"
    ),
    "member_is_us_noncitizen_national": (
        "us:regulations/7-cfr/273/4#input.member_is_us_noncitizen_national"
    ),
    "member_is_american_indian_born_in_canada_or_recognized_indian_tribe_member": (
        "us:regulations/7-cfr/273/4#input.member_is_american_indian_born_in_canada_or_recognized_indian_tribe_member"
    ),
    "member_is_hmong_or_highland_laotian_qualifying_person_or_family_member": (
        "us:regulations/7-cfr/273/4#input.member_is_hmong_or_highland_laotian_qualifying_person_or_family_member"
    ),
    "member_is_trafficking_victim_or_qualifying_family_member": (
        "us:regulations/7-cfr/273/4#input.member_is_trafficking_victim_or_qualifying_family_member"
    ),
    "member_is_qualified_alien_with_forty_qualifying_quarters": (
        "us:regulations/7-cfr/273/4#input.member_is_qualified_alien_with_forty_qualifying_quarters"
    ),
    "member_is_refugee": "us:regulations/7-cfr/273/4#input.member_is_refugee",
    "member_is_asylee": "us:regulations/7-cfr/273/4#input.member_is_asylee",
    "member_has_deportation_or_removal_withheld": (
        "us:regulations/7-cfr/273/4#input.member_has_deportation_or_removal_withheld"
    ),
    "member_is_cuban_or_haitian_entrant": (
        "us:regulations/7-cfr/273/4#input.member_is_cuban_or_haitian_entrant"
    ),
    "member_is_amerasian_immigrant": (
        "us:regulations/7-cfr/273/4#input.member_is_amerasian_immigrant"
    ),
    "member_has_eligible_military_connection": (
        "us:regulations/7-cfr/273/4#input.member_has_eligible_military_connection"
    ),
    "member_receives_blindness_or_disability_benefits": (
        "us:regulations/7-cfr/273/4#input.member_receives_blindness_or_disability_benefits"
    ),
    "member_was_lawfully_residing_on_1996_08_22_and_born_on_or_before_1931_08_22": (
        "us:regulations/7-cfr/273/4#input.member_was_lawfully_residing_on_1996_08_22_and_born_on_or_before_1931_08_22"
    ),
    "member_is_under_age_eighteen": (
        "us:regulations/7-cfr/273/4#input.member_is_under_age_eighteen"
    ),
    "member_is_qualified_alien_subject_to_five_year_wait": (
        "us:regulations/7-cfr/273/4#input.member_is_qualified_alien_subject_to_five_year_wait"
    ),
    "qualified_alien_five_year_status_period_met": (
        "us:regulations/7-cfr/273/4#input.qualified_alien_five_year_status_period_met"
    ),
    "alien_status_documentation_missing_or_unwilling": (
        "us:regulations/7-cfr/273/4#input.alien_status_documentation_missing_or_unwilling"
    ),
    "enrolled_at_least_half_time": (
        "us:regulations/7-cfr/273/5#input.enrolled_at_least_half_time"
    ),
    "enrolled_in_business_technical_trade_or_vocational_school_requiring_high_school_diploma": (
        "us:regulations/7-cfr/273/5#input.enrolled_in_business_technical_trade_or_vocational_school_requiring_high_school_diploma"
    ),
    "enrolled_in_college_or_university_degree_program": (
        "us:regulations/7-cfr/273/5#input.enrolled_in_college_or_university_degree_program"
    ),
    "student_age": "us:regulations/7-cfr/273/5#input.student_age",
    "student_physically_or_mentally_unfit": (
        "us:regulations/7-cfr/273/5#input.student_physically_or_mentally_unfit"
    ),
    "student_receives_tanf": (
        "us:regulations/7-cfr/273/5#input.student_receives_tanf"
    ),
    "enrolled_through_jobs_or_successor_program": (
        "us:regulations/7-cfr/273/5#input.enrolled_through_jobs_or_successor_program"
    ),
    "student_paid_employment_hours_per_week": (
        "us:regulations/7-cfr/273/5#input.student_paid_employment_hours_per_week"
    ),
    "student_self_employment_hours_per_week": (
        "us:regulations/7-cfr/273/5#input.student_self_employment_hours_per_week"
    ),
    "student_self_employment_weekly_earnings": (
        "us:regulations/7-cfr/273/5#input.student_self_employment_weekly_earnings"
    ),
    "federal_minimum_wage": (
        "us:regulations/7-cfr/273/5#input.federal_minimum_wage"
    ),
    "state_agency_averaged_student_work_hours_meet_twenty_per_week": (
        "us:regulations/7-cfr/273/5#input.state_agency_averaged_student_work_hours_meet_twenty_per_week"
    ),
    "student_participating_in_state_or_federally_financed_work_study": (
        "us:regulations/7-cfr/273/5#input.student_participating_in_state_or_federally_financed_work_study"
    ),
    "work_study_approved_at_snap_application": (
        "us:regulations/7-cfr/273/5#input.work_study_approved_at_snap_application"
    ),
    "work_study_approved_for_school_term": (
        "us:regulations/7-cfr/273/5#input.work_study_approved_for_school_term"
    ),
    "student_anticipates_working_in_work_study": (
        "us:regulations/7-cfr/273/5#input.student_anticipates_working_in_work_study"
    ),
    "work_study_exemption_period_active": (
        "us:regulations/7-cfr/273/5#input.work_study_exemption_period_active"
    ),
    "known_student_refused_work_study_assignment": (
        "us:regulations/7-cfr/273/5#input.known_student_refused_work_study_assignment"
    ),
    "student_participating_in_on_the_job_training_program": (
        "us:regulations/7-cfr/273/5#input.student_participating_in_on_the_job_training_program"
    ),
    "student_currently_being_trained_by_employer": (
        "us:regulations/7-cfr/273/5#input.student_currently_being_trained_by_employer"
    ),
    "responsible_for_care_of_dependent_household_member_under_age_six": (
        "us:regulations/7-cfr/273/5#input.responsible_for_care_of_dependent_household_member_under_age_six"
    ),
    "responsible_for_care_of_dependent_household_member_age_six_to_under_twelve": (
        "us:regulations/7-cfr/273/5#input.responsible_for_care_of_dependent_household_member_age_six_to_under_twelve"
    ),
    "adequate_child_care_unavailable_to_attend_class_and_meet_student_work_requirement": (
        "us:regulations/7-cfr/273/5#input.adequate_child_care_unavailable_to_attend_class_and_meet_student_work_requirement"
    ),
    "single_parent_enrolled_full_time_in_higher_education": (
        "us:regulations/7-cfr/273/5#input.single_parent_enrolled_full_time_in_higher_education"
    ),
    "responsible_for_care_of_dependent_child_under_twelve": (
        "us:regulations/7-cfr/273/5#input.responsible_for_care_of_dependent_child_under_twelve"
    ),
    "single_parent_household_condition_satisfied": (
        "us:regulations/7-cfr/273/5#input.single_parent_household_condition_satisfied"
    ),
    "assigned_or_placed_in_higher_education_through_qualifying_employment_training_program": (
        "us:regulations/7-cfr/273/5#input.assigned_or_placed_in_higher_education_through_qualifying_employment_training_program"
    ),
}

STUDENT_MEMBER_INPUT_DEFAULTS = {
    "enrolled_at_least_half_time": False,
    "enrolled_in_business_technical_trade_or_vocational_school_requiring_high_school_diploma": False,
    "enrolled_in_college_or_university_degree_program": False,
    "student_age": 50,
    "student_physically_or_mentally_unfit": False,
    "student_receives_tanf": False,
    "enrolled_through_jobs_or_successor_program": False,
    "student_paid_employment_hours_per_week": 0,
    "student_self_employment_hours_per_week": 0,
    "student_self_employment_weekly_earnings": 0,
    "federal_minimum_wage": 7.25,
    "state_agency_averaged_student_work_hours_meet_twenty_per_week": False,
    "student_participating_in_state_or_federally_financed_work_study": False,
    "work_study_approved_at_snap_application": False,
    "work_study_approved_for_school_term": False,
    "student_anticipates_working_in_work_study": False,
    "work_study_exemption_period_active": False,
    "known_student_refused_work_study_assignment": False,
    "student_participating_in_on_the_job_training_program": False,
    "student_currently_being_trained_by_employer": False,
    "responsible_for_care_of_dependent_household_member_under_age_six": False,
    "responsible_for_care_of_dependent_household_member_age_six_to_under_twelve": False,
    "adequate_child_care_unavailable_to_attend_class_and_meet_student_work_requirement": False,
    "single_parent_enrolled_full_time_in_higher_education": False,
    "responsible_for_care_of_dependent_child_under_twelve": False,
    "single_parent_household_condition_satisfied": False,
    "assigned_or_placed_in_higher_education_through_qualifying_employment_training_program": False,
}

CITIZENSHIP_MEMBER_INPUT_DEFAULTS = {
    "member_is_us_citizen": False,
    "member_is_us_noncitizen_national": False,
    "member_is_american_indian_born_in_canada_or_recognized_indian_tribe_member": False,
    "member_is_hmong_or_highland_laotian_qualifying_person_or_family_member": False,
    "member_is_trafficking_victim_or_qualifying_family_member": False,
    "member_is_qualified_alien_with_forty_qualifying_quarters": False,
    "member_is_refugee": False,
    "member_is_asylee": False,
    "member_has_deportation_or_removal_withheld": False,
    "member_is_cuban_or_haitian_entrant": False,
    "member_is_amerasian_immigrant": False,
    "member_has_eligible_military_connection": False,
    "member_receives_blindness_or_disability_benefits": False,
    "member_was_lawfully_residing_on_1996_08_22_and_born_on_or_before_1931_08_22": False,
    "member_is_under_age_eighteen": False,
    "member_is_qualified_alien_subject_to_five_year_wait": False,
    "qualified_alien_five_year_status_period_met": False,
    "alien_status_documentation_missing_or_unwilling": False,
}

GENERAL_WORK_MEMBER_AGE_INPUT = "us:regulations/7-cfr/273/7#input.member_age"
ABAWD_MEMBER_AGE_INPUT = "us:regulations/7-cfr/273/24#input.member_age"
WORK_MEMBER_ELIGIBLE_INPUTS = {
    GENERAL_WORK_MEMBER_AGE_INPUT: 60,
    ABAWD_MEMBER_AGE_INPUT: 60,
}
WORK_MEMBER_INELIGIBLE_INPUTS = {
    GENERAL_WORK_MEMBER_AGE_INPUT: 30,
    "us:regulations/7-cfr/273/7#input.member_age_16_or_17_is_not_household_head_or_attends_school_or_training_half_time": False,
    "us:regulations/7-cfr/273/7#input.member_physically_or_mentally_unfit_for_employment": False,
    "us:regulations/7-cfr/273/7#input.member_subject_to_and_complying_with_title_iv_work_requirement": False,
    "us:regulations/7-cfr/273/7#input.member_responsible_for_dependent_child_under_six_or_incapacitated_person": False,
    "us:regulations/7-cfr/273/7#input.member_receiving_or_applying_for_unemployment_compensation_and_complying": False,
    "us:regulations/7-cfr/273/7#input.member_regular_participant_in_drug_or_alcohol_treatment": False,
    "us:regulations/7-cfr/273/7#input.member_weekly_work_hours": 0,
    "us:regulations/7-cfr/273/7#input.member_weekly_wages": 0,
    "us:regulations/7-cfr/273/7#input.federal_or_state_minimum_wage": 7.25,
    "us:regulations/7-cfr/273/7#input.migrant_or_seasonal_farmworker_under_contract_to_begin_employment_within_30_days": False,
    "us:regulations/7-cfr/273/7#input.alaska_subsistence_hunts_or_fishes_30_hours_weekly": False,
    "us:regulations/7-cfr/273/7#input.member_student_enrolled_at_least_half_time_and_student_eligible": False,
    "us:regulations/7-cfr/273/7#input.member_snap_work_requirements_waived_due_to_pending_ssi_joint_application": False,
    "us:regulations/7-cfr/273/7#input.member_registered_for_work_or_registered_by_state": False,
    "us:regulations/7-cfr/273/7#input.member_participated_in_snap_et_if_assigned": True,
    "us:regulations/7-cfr/273/7#input.member_participated_in_workfare_if_assigned": True,
    "us:regulations/7-cfr/273/7#input.member_provided_employment_status_or_availability_information": True,
    "us:regulations/7-cfr/273/7#input.member_reported_to_referred_suitable_employer_if_referred": True,
    "us:regulations/7-cfr/273/7#input.member_accepted_bona_fide_suitable_employment_offer_if_offered": True,
    "us:regulations/7-cfr/273/7#input.member_voluntarily_quit_or_reduced_work_below_30_hours_without_good_cause": False,
    ABAWD_MEMBER_AGE_INPUT: 30,
    "us:regulations/7-cfr/273/24#input.member_medically_certified_physically_or_mentally_unfit_for_employment": False,
    "us:regulations/7-cfr/273/24#input.member_is_parent_of_household_member_under_age_eighteen": False,
    "us:regulations/7-cfr/273/24#input.member_resides_with_household_member_under_age_eighteen": False,
    "us:regulations/7-cfr/273/24#input.member_is_pregnant": False,
    "us:regulations/7-cfr/273/24#input.member_is_homeless": False,
    "us:regulations/7-cfr/273/24#input.member_is_veteran": False,
    "us:regulations/7-cfr/273/24#input.member_age_24_or_younger_and_was_in_foster_care_on_attaining_age_18": False,
    "us:regulations/7-cfr/273/24#input.member_covered_by_abawd_time_limit_waiver": False,
    "us:regulations/7-cfr/273/24#input.member_abawd_weekly_work_hours": 0,
    "us:regulations/7-cfr/273/24#input.member_abawd_monthly_work_hours": 0,
    "us:regulations/7-cfr/273/24#input.member_participates_in_abawd_work_program_20_hours_weekly": False,
    "us:regulations/7-cfr/273/24#input.member_combines_work_and_work_program_20_hours_weekly": False,
    "us:regulations/7-cfr/273/24#input.member_participates_in_abawd_workfare_program": False,
    "us:regulations/7-cfr/273/24#input.snap_abawd_countable_months_in_three_year_period": 4,
    "us:regulations/7-cfr/273/24#input.member_regained_abawd_eligibility": False,
    "us:regulations/7-cfr/273/24#input.member_has_additional_three_month_abawd_eligibility": False,
}


@dataclass(frozen=True)
class Period:
    label: str
    year: int
    month: int
    start: date
    end: date


@dataclass
class ProjectedCase:
    spm_unit_id: int
    household_id: int
    inputs: dict[str, Any]
    member_inputs: list[dict[str, Any]]
    pe_outputs: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--month", type=int, default=1)
    parser.add_argument("--state", default="CO")
    parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Limit after state filtering. Omit to run all matching eCPS SPM units.",
    )
    parser.add_argument(
        "--positive-snap-only",
        action="store_true",
        help=(
            "Only compare eCPS SPM units where PolicyEngine normal allotment "
            "is positive."
        ),
    )
    parser.add_argument(
        "--utility-projection",
        choices=("raw-expenses", "policyengine-type"),
        default="raw-expenses",
        help=(
            "How to project eCPS utility facts. raw-expenses uses itemized "
            "utility expenses from eCPS. policyengine-type maps "
            "PolicyEngine's utility-allowance type into Colorado utility "
            "facts for oracle parity."
        ),
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=1.5,
        help=(
            "Dollar tolerance for matching PE. Defaults to 1.5 because PE's "
            "normal allotment can retain fractional dollars while RuleSpec "
            "floors final allotments to whole dollars."
        ),
    )
    parser.add_argument("--max-differences", type=int, default=20)
    parser.add_argument(
        "--fail-on-mismatch",
        action="store_true",
        help="Exit nonzero when any row differs by more than --tolerance.",
    )
    parser.add_argument("--program", type=Path, default=DEFAULT_PROGRAM)
    parser.add_argument("--test-template", type=Path, default=DEFAULT_TEST_TEMPLATE)
    parser.add_argument("--axiom-binary", type=Path, default=DEFAULT_AXIOM_BINARY)
    parser.add_argument("--write-csv", type=Path, default=None)
    return parser.parse_args()


def month_period(year: int, month: int) -> Period:
    return Period(
        label=f"{year:04d}-{month:02d}",
        year=year,
        month=month,
        start=date(year, month, 1),
        end=date(year, month, monthrange(year, month)[1]),
    )


def load_base_inputs(path: Path) -> dict[str, Any]:
    cases = yaml.safe_load(path.read_text())
    if not isinstance(cases, list) or not cases:
        raise ValueError(f"{path} must contain at least one test case")
    inputs = cases[0].get("input")
    if not isinstance(inputs, dict):
        raise ValueError(f"{path} first test case must contain an input mapping")
    return {
        str(reference): value
        for reference, value in inputs.items()
        if "#relation." not in str(reference)
    }


def _friendly_input_name(reference: str) -> str | None:
    marker = "#input."
    if marker not in reference:
        return None
    return reference.split(marker, 1)[1]


def legal_input_index(inputs: dict[str, Any]) -> dict[str, str]:
    index: dict[str, str] = {}
    for reference in inputs:
        name = _friendly_input_name(str(reference))
        if name:
            index[name] = str(reference)
    return index


def legalize_inputs(
    inputs: dict[str, Any],
    reference_by_name: dict[str, str],
) -> dict[str, Any]:
    legal: dict[str, Any] = {}
    for name, value in inputs.items():
        if "#" in name and ":" in name:
            reference = name
        else:
            reference = reference_by_name.get(name)
            if reference is None:
                raise KeyError(f"no legal RuleSpec input reference for `{name}`")
        legal[reference] = value
    return legal


def project_student_member_inputs(student_eligible: bool) -> dict[str, Any]:
    inputs = dict(STUDENT_MEMBER_INPUT_DEFAULTS)
    if not student_eligible:
        inputs.update(
            {
                "enrolled_at_least_half_time": True,
                "enrolled_in_college_or_university_degree_program": True,
                "student_age": 20,
            }
        )
    return inputs


def project_citizenship_member_inputs(immigration_eligible: bool) -> dict[str, Any]:
    inputs = dict(CITIZENSHIP_MEMBER_INPUT_DEFAULTS)
    if immigration_eligible:
        inputs["member_is_us_citizen"] = True
    return inputs


def project_work_member_inputs(work_eligible: bool) -> dict[str, Any]:
    if work_eligible:
        return dict(WORK_MEMBER_ELIGIBLE_INPUTS)
    return dict(WORK_MEMBER_INELIGIBLE_INPUTS)


def array(values: Any) -> np.ndarray:
    if hasattr(values, "to_numpy"):
        return values.to_numpy()
    if hasattr(values, "values"):
        return np.asarray(values.values)
    return np.asarray(values)


def calculate(sim: Any, name: str, period: str | int) -> np.ndarray:
    return array(sim.calculate(name, period))


def any_by_id(ids: np.ndarray, values: np.ndarray) -> dict[int, bool]:
    result: dict[int, bool] = {}
    for raw_id, value in zip(ids, values, strict=False):
        key = int(raw_id)
        result[key] = bool(result.get(key, False) or bool(value))
    return result


def build_state_map(
    sim: Any, year: int, spm_ids: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    person_spm = calculate(sim, "person_spm_unit_id", year)
    person_household = calculate(sim, "person_household_id", year)
    household_ids = calculate(sim, "household_id", year)
    household_states = calculate(sim, "state_code_str", year).astype(str)

    household_state_by_id = {
        int(household_id): state
        for household_id, state in zip(household_ids, household_states, strict=False)
    }
    spm_household_by_id: dict[int, int] = {}
    for spm_id, household_id in zip(person_spm, person_household, strict=False):
        spm_household_by_id.setdefault(int(spm_id), int(household_id))

    states = np.array(
        [
            household_state_by_id.get(spm_household_by_id.get(int(spm_id), -1), "")
            for spm_id in spm_ids
        ]
    )
    household_for_spm = np.array(
        [spm_household_by_id.get(int(spm_id), -1) for spm_id in spm_ids],
        dtype=np.int64,
    )
    return states, household_for_spm


def load_policyengine_cases(
    *,
    base_inputs: dict[str, Any],
    period: Period,
    state: str,
    sample_size: int | None,
    positive_snap_only: bool,
    utility_projection: str,
) -> list[ProjectedCase]:
    try:
        from policyengine_us import Microsimulation
    except ImportError as exc:
        raise SystemExit(
            "policyengine-us is required. Run with: "
            "uv run --with policyengine-us --with pyyaml "
            "scripts/compare_snap_policyengine_ecps.py"
        ) from exc

    print("Loading PolicyEngine enhanced CPS...")
    sim = Microsimulation()

    period_label = period.label
    year = period.year
    spm_ids = calculate(sim, "spm_unit_id", year)
    spm_unit_size = calculate(sim, "spm_unit_size", year)
    snap_unit_size = calculate(sim, "snap_unit_size", period_label)
    states, household_ids = build_state_map(sim, year, spm_ids)

    pe_snap = calculate(sim, PE_COMPARED_OUTPUT, period_label)
    state_mask = states == state
    valid_size_mask = snap_unit_size >= 1
    skipped_empty_units = int(np.count_nonzero(state_mask & ~valid_size_mask))
    mask = state_mask & valid_size_mask
    if positive_snap_only:
        mask &= pe_snap > 0

    indices = np.flatnonzero(mask)
    if sample_size is not None:
        indices = indices[:sample_size]

    print(f"Projecting {len(indices):,} {state} eCPS SPM units...")
    if skipped_empty_units:
        print(
            f"Skipped {skipped_empty_units:,} {state} eCPS SPM units "
            "with SNAP unit size < 1."
        )

    person_spm = calculate(sim, "person_spm_unit_id", year)
    student_ok_by_spm = any_by_id(
        person_spm,
        ~calculate(sim, "is_snap_ineligible_student", year).astype(bool),
    )
    immigration_ok_by_spm = any_by_id(
        person_spm,
        calculate(sim, "is_snap_immigration_status_eligible", period_label).astype(
            bool
        ),
    )

    values = {
        "spm_unit_size": spm_unit_size,
        "snap_unit_size": snap_unit_size,
        PE_COMPARED_OUTPUT: pe_snap,
        "snap": calculate(sim, "snap", period_label),
        "is_snap_eligible": calculate(sim, "is_snap_eligible", period_label),
        "snap_gross_income": calculate(sim, "snap_gross_income", period_label),
        "snap_earned_income": calculate(sim, "snap_earned_income", period_label),
        "snap_unearned_income": calculate(sim, "snap_unearned_income", period_label),
        "snap_net_income": calculate(sim, "snap_net_income", period_label),
        "snap_max_allotment": calculate(sim, "snap_max_allotment", period_label),
        "snap_standard_deduction": calculate(
            sim, "snap_standard_deduction", period_label
        ),
        "snap_earned_income_deduction": calculate(
            sim, "snap_earned_income_deduction", period_label
        ),
        "snap_dependent_care_deduction": calculate(
            sim, "snap_dependent_care_deduction", period_label
        ),
        "snap_child_support_deduction": calculate(
            sim, "snap_child_support_deduction", period_label
        ),
        "snap_excess_medical_expense_deduction": calculate(
            sim, "snap_excess_medical_expense_deduction", period_label
        ),
        "snap_utility_allowance": calculate(
            sim, "snap_utility_allowance", period_label
        ),
        "snap_utility_allowance_type": calculate(
            sim, "snap_utility_allowance_type", period_label
        ),
        "snap_excess_shelter_expense_deduction": calculate(
            sim, "snap_excess_shelter_expense_deduction", period_label
        ),
        "housing_cost": calculate(sim, "housing_cost", period_label),
        "snap_assets": calculate(sim, "snap_assets", year),
        "has_usda_elderly_disabled": calculate(
            sim, "has_usda_elderly_disabled", period_label
        ),
        "meets_snap_categorical_eligibility": calculate(
            sim, "meets_snap_categorical_eligibility", period_label
        ),
        "meets_snap_work_requirements": calculate(
            sim, "meets_snap_work_requirements", period_label
        ),
        "heating_cooling_expense": calculate(sim, "heating_cooling_expense", year),
        "pre_subsidy_electricity_expense": calculate(
            sim, "pre_subsidy_electricity_expense", year
        ),
        "gas_expense": calculate(sim, "gas_expense", year),
        "phone_expense": calculate(sim, "phone_expense", year),
        "trash_expense": calculate(sim, "trash_expense", year),
        "water_expense": calculate(sim, "water_expense", year),
        "sewage_expense": calculate(sim, "sewage_expense", year),
    }

    household_input_ref_by_name = legal_input_index(base_inputs)
    member_input_ref_by_name = dict(AXIOM_MEMBER_INPUT_ID_BY_LABEL)

    cases: list[ProjectedCase] = []
    for idx in indices:
        spm_id = int(spm_ids[idx])
        utility_inputs = {
            "household_incurred_or_anticipated_heating_or_cooling_costs_separate_from_rent_or_mortgage": bool(
                values["heating_cooling_expense"][idx] > 0
            ),
            "household_pays_electricity_utility_cost": bool(
                values["pre_subsidy_electricity_expense"][idx] > 0
            ),
            "household_pays_water_utility_cost": bool(values["water_expense"][idx] > 0),
            "household_pays_sewer_utility_cost": bool(
                values["sewage_expense"][idx] > 0
            ),
            "household_pays_trash_utility_cost": bool(values["trash_expense"][idx] > 0),
            "household_pays_cooking_fuel_utility_cost": bool(
                values["gas_expense"][idx] > 0
            ),
            "household_pays_telephone_service_cost": bool(
                values["phone_expense"][idx] > 0
            ),
        }
        if utility_projection == "policyengine-type":
            utility_inputs = project_utility_allowance_type(
                str(native(values["snap_utility_allowance_type"][idx]))
            )
        dependent_care_deduction = money(values["snap_dependent_care_deduction"][idx])
        child_support_deduction = money(values["snap_child_support_deduction"][idx])
        medical_deduction = money(values["snap_excess_medical_expense_deduction"][idx])

        inputs = dict(base_inputs)
        inputs.update(
            {
                "household_size": int(values["snap_unit_size"][idx]),
                "employee_wages_received": money(values["snap_earned_income"][idx]),
                "other_gain_or_benefit_payments": money(
                    values["snap_unearned_income"][idx]
                ),
                "household_shelter_costs_incurred": money(values["housing_cost"][idx]),
                "liquid_resource_current_redemption_rate": money(
                    values["snap_assets"][idx]
                ),
                "snap_basic_categorical_eligible": bool(
                    values["meets_snap_categorical_eligibility"][idx]
                ),
                "snap_expanded_categorical_eligible": False,
                "household_lives_in_application_state": True,
                "household_in_project_area_solely_for_vacation": False,
                "household_contains_individual_participating_in_more_than_one_household_or_project_area": False,
                "resident_of_battered_women_and_children_shelter_and_prior_abusive_household_member": False,
                "dependent_care_expense_necessary_for_work_or_training": (
                    dependent_care_deduction > 0
                ),
                "dependent_care_expenses_paid": dependent_care_deduction,
                "dependent_care_reimbursed_or_paid_by_other_program": 0,
                "child_support_payment_verified": child_support_deduction > 0,
                "child_support_payment_history_months": (
                    3 if child_support_deduction > 0 else 0
                ),
                "average_monthly_child_support_paid": child_support_deduction,
                "estimated_monthly_child_support_paid": child_support_deduction,
                "total_medical_expenses": medical_expenses_for_deduction(
                    medical_deduction
                ),
                **utility_inputs,
            }
        )
        member_inputs = project_student_member_inputs(
            bool(student_ok_by_spm.get(spm_id, False))
        )
        member_inputs.update(
            project_citizenship_member_inputs(
                bool(immigration_ok_by_spm.get(spm_id, False))
            )
        )
        member_inputs.update(
            project_work_member_inputs(bool(values["meets_snap_work_requirements"][idx]))
        )
        member_inputs["snap_member_is_elderly_or_disabled"] = bool(
            values["has_usda_elderly_disabled"][idx]
        )
        cases.append(
            ProjectedCase(
                spm_unit_id=spm_id,
                household_id=int(household_ids[idx]),
                inputs=legalize_inputs(inputs, household_input_ref_by_name),
                member_inputs=[
                    legalize_inputs(
                        member_inputs,
                        member_input_ref_by_name,
                    )
                ],
                pe_outputs={name: native(values[name][idx]) for name in values},
            )
        )

    return cases


def project_utility_allowance_type(utility_type: str) -> dict[str, bool]:
    inputs = {
        "household_incurred_or_anticipated_heating_or_cooling_costs_separate_from_rent_or_mortgage": False,
        "household_pays_electricity_utility_cost": False,
        "household_pays_water_utility_cost": False,
        "household_pays_sewer_utility_cost": False,
        "household_pays_trash_utility_cost": False,
        "household_pays_cooking_fuel_utility_cost": False,
        "household_pays_telephone_service_cost": False,
    }
    if utility_type == "SUA":
        inputs[
            "household_incurred_or_anticipated_heating_or_cooling_costs_separate_from_rent_or_mortgage"
        ] = True
    elif utility_type == "LUA":
        inputs["household_pays_electricity_utility_cost"] = True
        inputs["household_pays_water_utility_cost"] = True
    elif utility_type == "IUA":
        inputs["household_pays_electricity_utility_cost"] = True
    return inputs


def medical_expenses_for_deduction(deduction: float) -> float:
    if deduction <= 0:
        return 0
    if deduction <= 165:
        return 36
    return deduction + 35


def money(value: Any) -> float:
    value = float(value)
    if not math.isfinite(value):
        return 0.0
    return round(value, 6)


def native(value: Any) -> Any:
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float):
        return money(value)
    return value


def scalar_value(value: Any) -> dict[str, Any]:
    value = native(value)
    if isinstance(value, bool):
        return {"kind": "bool", "value": value}
    if isinstance(value, int) and not isinstance(value, bool):
        return {"kind": "integer", "value": value}
    if isinstance(value, float):
        return {"kind": "decimal", "value": decimal_literal(value)}
    if isinstance(value, str):
        if len(value) == 10 and value[4] == "-" and value[7] == "-":
            return {"kind": "date", "value": value}
        return {"kind": "text", "value": value}
    raise TypeError(f"unsupported input value {value!r}")


def decimal_literal(value: float) -> str:
    literal = f"{value:.6f}".rstrip("0").rstrip(".")
    return literal or "0"


def compile_program(binary: Path, program: Path, output: Path) -> None:
    result = subprocess.run(
        [
            str(binary),
            "compile",
            "--program",
            str(program),
            "--output",
            str(output),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())


def run_axiom_cases(
    *,
    binary: Path,
    artifact: Path,
    cases: list[ProjectedCase],
    period: Period,
) -> list[dict[str, Any]]:
    interval = {
        "start": period.start.isoformat(),
        "end": period.end.isoformat(),
    }
    period_json = {
        "period_kind": "month",
        "start": period.start.isoformat(),
        "end": period.end.isoformat(),
        "name": period.label,
    }
    inputs = []
    relations = []
    queries = []
    for case in cases:
        entity_id = f"spm-{case.spm_unit_id}"
        for name, value in case.inputs.items():
            inputs.append(
                {
                    "name": name,
                    "entity": "Household",
                    "entity_id": entity_id,
                    "interval": interval,
                    "value": scalar_value(value),
                }
            )
        for member_index, member_inputs in enumerate(case.member_inputs, 1):
            member_entity_id = f"{entity_id}-member-{member_index}"
            relations.append(
                {
                    "name": AXIOM_RELATION_ID_BY_LABEL["member_of_household"],
                    "tuple": [member_entity_id, entity_id],
                    "interval": interval,
                }
            )
            for name, value in member_inputs.items():
                inputs.append(
                    {
                        "name": name,
                        "entity": "Member",
                        "entity_id": member_entity_id,
                        "interval": interval,
                        "value": scalar_value(value),
                    }
                )
        queries.append(
            {
                "entity_id": entity_id,
                "period": period_json,
                "outputs": AXIOM_OUTPUTS,
            }
        )

    request = {
        "mode": "fast",
        "dataset": {"inputs": inputs, "relations": relations},
        "queries": queries,
    }
    result = subprocess.run(
        [str(binary), "run-compiled", "--artifact", str(artifact)],
        input=json.dumps(request),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    payload = json.loads(result.stdout)
    return payload["results"]


def output_to_python(output: dict[str, Any]) -> Any:
    if output.get("kind") == "judgment":
        return output.get("outcome")
    value = output.get("value", {})
    raw = value.get("value")
    if value.get("kind") == "decimal":
        return float(raw)
    return raw


def outputs_by_reference(outputs: dict[str, Any]) -> dict[str, dict[str, Any]]:
    references: dict[str, dict[str, Any]] = {}
    for output_key, output in outputs.items():
        if not isinstance(output, dict):
            continue
        references[str(output_key)] = output
        output_id = str(output.get("id") or "").strip()
        if output_id:
            references[output_id] = output
    return references


def compare(
    cases: list[ProjectedCase], results: list[dict[str, Any]], tolerance: float
):
    rows = []
    for case, result in zip(cases, results, strict=True):
        raw_outputs = result.get("outputs", {})
        if not isinstance(raw_outputs, dict):
            raise ValueError(
                f"Axiom result for SPM unit {case.spm_unit_id} has no outputs"
            )
        output_references = outputs_by_reference(raw_outputs)
        missing_outputs = sorted(
            output_id
            for output_id in AXIOM_OUTPUT_ID_BY_LABEL.values()
            if output_id not in output_references
        )
        if missing_outputs:
            joined = ", ".join(missing_outputs)
            raise ValueError(
                f"Axiom result for SPM unit {case.spm_unit_id} is missing {joined}"
            )
        outputs = {
            label: output_to_python(output_references[output_id])
            for label, output_id in AXIOM_OUTPUT_ID_BY_LABEL.items()
        }
        axiom_snap = float(outputs[COMPARED_AXIOM_OUTPUT])
        pe_snap = float(case.pe_outputs[PE_COMPARED_OUTPUT])
        diff = axiom_snap - pe_snap
        rows.append(
            {
                "spm_unit_id": case.spm_unit_id,
                "household_id": case.household_id,
                "pe_snap": pe_snap,
                "axiom_snap_allotment": axiom_snap,
                "difference": diff,
                "absolute_difference": abs(diff),
                "match": abs(diff) <= tolerance,
                "pe_snap_eligible": bool(case.pe_outputs["is_snap_eligible"]),
                "axiom_snap_eligible": outputs["snap_eligible"],
                "pe_gross_income": case.pe_outputs["snap_gross_income"],
                "axiom_gross_income": outputs["snap_gross_monthly_income"],
                "pe_net_income": case.pe_outputs["snap_net_income"],
                "axiom_net_income": outputs["snap_net_income"],
                "pe_max_allotment": case.pe_outputs["snap_max_allotment"],
                "axiom_max_allotment": outputs["snap_maximum_allotment"],
                "pe_utility_allowance": case.pe_outputs["snap_utility_allowance"],
                "axiom_utility_allowance": sum(
                    float(outputs[name])
                    for name in [
                        "snap_standard_utility_allowance",
                        "snap_limited_utility_allowance",
                        "snap_one_utility_allowance",
                        "snap_individual_utility_allowance",
                    ]
                ),
                "pe_shelter_deduction": case.pe_outputs[
                    "snap_excess_shelter_expense_deduction"
                ],
                "axiom_shelter_deduction": outputs["snap_excess_shelter_deduction"],
            }
        )
    return rows


def print_summary(
    rows: list[dict[str, Any]], tolerance: float, max_differences: int
) -> None:
    total = len(rows)
    matches = sum(1 for row in rows if row["match"])
    diffs = sorted(rows, key=lambda row: row["absolute_difference"], reverse=True)
    mean_abs = sum(row["absolute_difference"] for row in rows) / total if total else 0.0
    print()
    print(f"Compared {total:,} PolicyEngine eCPS SPM units")
    print(f"Tolerance: ${tolerance:,.2f}")
    print(
        f"Matches: {matches:,}/{total:,} ({matches / total:.1%})"
        if total
        else "No rows"
    )
    print(f"Mean absolute difference: ${mean_abs:,.2f}")
    if diffs:
        print(f"Max absolute difference: ${diffs[0]['absolute_difference']:,.2f}")
    print()
    print(f"Top {min(max_differences, len(diffs))} differences:")
    for row in diffs[:max_differences]:
        print(
            "  "
            f"spm={row['spm_unit_id']} "
            f"PE=${row['pe_snap']:.2f} Axiom=${row['axiom_snap_allotment']:.2f} "
            f"diff=${row['difference']:.2f} "
            f"eligible PE={row['pe_snap_eligible']} Axiom={row['axiom_snap_eligible']} "
            f"gross PE=${row['pe_gross_income']:.2f} Axiom=${row['axiom_gross_income']:.2f} "
            f"net PE=${row['pe_net_income']:.2f} Axiom=${row['axiom_net_income']:.2f} "
            f"utility PE=${row['pe_utility_allowance']:.2f} Axiom=${row['axiom_utility_allowance']:.2f}"
        )


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    period = month_period(args.year, args.month)
    base_inputs = load_base_inputs(args.test_template)
    print(f"Utility projection: {args.utility_projection}")
    cases = load_policyengine_cases(
        base_inputs=base_inputs,
        period=period,
        state=args.state,
        sample_size=args.sample_size,
        positive_snap_only=args.positive_snap_only,
        utility_projection=args.utility_projection,
    )
    if not cases:
        print("No matching eCPS SPM units.")
        return 1

    with tempfile.TemporaryDirectory(prefix="co-snap-pe-ecps-") as temp_dir:
        artifact = Path(temp_dir) / "program.compiled.json"
        print("Compiling Colorado SNAP RuleSpec composition...")
        compile_program(args.axiom_binary, args.program, artifact)
        print("Running Axiom Rules over projected eCPS records...")
        results = run_axiom_cases(
            binary=args.axiom_binary,
            artifact=artifact,
            cases=cases,
            period=period,
        )

    rows = compare(cases, results, args.tolerance)
    print_summary(rows, args.tolerance, args.max_differences)
    if args.write_csv is not None:
        write_csv(args.write_csv, rows)
        print(f"Wrote {args.write_csv}")

    if args.fail_on_mismatch and not all(row["match"] for row in rows):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
