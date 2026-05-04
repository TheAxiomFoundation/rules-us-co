#!/usr/bin/env python3
"""Compare Colorado SNAP RuleSpec output against PolicyEngine enhanced CPS.

The script projects PolicyEngine eCPS SPM-unit records into the current
Colorado SNAP composition input surface, including related member facts, runs
the Axiom Rules engine once over those projected records, and compares regular
monthly SNAP allotments against PolicyEngine's normal allotment. It uses these
targets because the enhanced CPS records do not include application dates for
initial-month proration, and PolicyEngine's top-level ``snap`` microsimulation
value includes take-up adjustments.
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
    "gross_income": (
        "us-co:policies/cdhs/snap/fy-2026-benefit-calculation#gross_income"
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
    "excess_shelter_deduction": (
        "us-co:regulations/10-ccr-2506-1/4.407.3#excess_shelter_deduction"
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
        "--project-policyengine-utility-allowance",
        action="store_true",
        help=(
            "Project PolicyEngine's eCPS utility allowance type into the "
            "closest Colorado utility facts. Useful for benefit parity when "
            "eCPS has no itemized utility expenses."
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
    return dict(inputs)


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
    project_policyengine_utility_allowance: bool,
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
        if project_policyengine_utility_allowance:
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
                "snap_work_requirement_eligible": bool(
                    values["meets_snap_work_requirements"][idx]
                ),
                "snap_student_eligible": bool(student_ok_by_spm.get(spm_id, False)),
                "snap_residency_citizenship_eligible": bool(
                    immigration_ok_by_spm.get(spm_id, False)
                ),
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
        cases.append(
            ProjectedCase(
                spm_unit_id=spm_id,
                household_id=int(household_ids[idx]),
                inputs=legalize_inputs(inputs, household_input_ref_by_name),
                member_inputs=[
                    legalize_inputs(
                        {
                            "snap_member_is_elderly_or_disabled": bool(
                                values["has_usda_elderly_disabled"][idx]
                            )
                        },
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
                "axiom_gross_income": outputs["gross_income"],
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
                "axiom_shelter_deduction": outputs["excess_shelter_deduction"],
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
    cases = load_policyengine_cases(
        base_inputs=base_inputs,
        period=period,
        state=args.state,
        sample_size=args.sample_size,
        positive_snap_only=args.positive_snap_only,
        project_policyengine_utility_allowance=args.project_policyengine_utility_allowance,
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
