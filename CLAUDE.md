# rac-us-co

Colorado benefit-program encodings live here.

## Scope

- Colorado administrative regulations, manuals, and guidance
- first source: `9 CCR 2503-6` Colorado Works Program
- keep statute companions under `statute/` and sync both trees into Atlas
- sync the broader official Colorado Works source tree into Atlas when source snapshots exist
- current encoded scope includes earned-income, certification-period, grant-calculation,
  assistance-unit, countable-income, income, and IRC leaves

## Layout

```text
rac-us-co/
├── regulation/        # Colorado regulations and rule manuals
├── statute/           # Colorado statutes when needed for imports
├── sources/
│   ├── official/      # full PDF + AKN snapshots
│   └── slices/        # exact clause text for atomic leaves
└── waves/             # wave provenance manifests
```

## Local commands

```bash
cd /Users/maxghenis/TheAxiomFoundation/rac
uv run python -m rac.validate all /Users/maxghenis/TheAxiomFoundation/rac-us-co
uv run python -m rac.test_runner /Users/maxghenis/TheAxiomFoundation/rac-us-co -v

cd /Users/maxghenis/TheAxiomFoundation/rac-us-co
python3 scripts/validate_repo.py
python3 scripts/sync_atlas.py
```

## Encoding policy

- Prefer the most atomic rule slice possible.
- If a leaf is derived from a larger manual subsection, keep the exact excerpt in
  `sources/slices/`.
- Do not include the local subsection or paragraph citation in symbol names; the
  file path already provides that context for imports.
- Do not invent convenience scalars inside formulas; every substantive number should
  be its own variable.
- If a rule depends on statute text, add or import the statute companion instead of
  paraphrasing it locally.
- Do not leave promoted corpus files as `status: stub`.
  - Stub only the RAC layer during generation, never the source layer.
  - Once the official source is ingested locally, encode the upstream file before promotion.
