# rules-us-co

Colorado RuleSpec encodings and source registry metadata.

## Contents

- `sources/`: source registry or manifest metadata when needed.
- `statutes/`, `regulations/`, or `policies/`: RuleSpec YAML when encoded rules are added.
- `.github/workflows/`: wrapper around the shared RuleSpec validation workflow.

## Conventions

Use RuleSpec YAML under `statutes/`, `regulations/`, or `policies/` for encoded rules. Do not add source text, generated source payloads, or extracted document snapshots to Git; source text belongs in the corpus database/object storage, with only registry or manifest metadata here when needed.

Jurisdiction-specific materials belong in this repo. Shared federal materials belong in `rules-us`.
