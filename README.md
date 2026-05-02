# rules-us-co

Colorado RuleSpec encodings.

## Contents

- `statutes/`, `regulations/`, or `policies/`: RuleSpec YAML when encoded rules are added.
- `.github/workflows/`: wrapper around the shared RuleSpec validation workflow.

## Conventions

Use RuleSpec YAML under `statutes/`, `regulations/`, or `policies/` for encoded rules. Do not add source text, source registry sidecars, generated source payloads, extracted document snapshots, or wave manifests to Git; source material belongs in the corpus database/object storage.

Jurisdiction-specific materials belong in this repo. Shared federal materials belong in `rules-us`.

## PolicyEngine eCPS SNAP Comparison

Compare the Colorado SNAP composition against PolicyEngine enhanced CPS records:

```bash
uv run --with policyengine-us --with pyyaml \
  scripts/compare_snap_policyengine_ecps.py \
  --project-policyengine-utility-allowance
```

The comparison uses PolicyEngine's `snap_normal_allotment`, not top-level `snap`,
because microsimulation `snap` includes take-up adjustments. It compares against
RuleSpec `us:statutes/7/2017/a#snap_regular_month_allotment` because eCPS does
not include application-date facts for initial-month proration.

For eCPS parity, `--project-policyengine-utility-allowance` maps PE's utility
allowance type and aggregate deduction values into the closest Colorado input
facts. The comparison projects PE's elderly-or-disabled SNAP status onto a
related member fact, and the RuleSpec derives the household-level status through
`member_of_household`. The live RuleSpec computation still derives utility,
medical, child support, dependent care, shelter, eligibility, and allotment
values from the encoded rules.
