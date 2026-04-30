# rules-us-co Agent Notes

This repo stores Colorado RuleSpec encodings and source registry metadata.

## Do

- Keep source text and document snapshots in the corpus database/object storage, not in Git.
- Add or update metadata-only source registry files when provenance or relation metadata is needed.
- Add RuleSpec encodings under `statutes/`, `regulations/`, or `policies/` when ready.
- Keep parameter tables as structured YAML when they are useful reference data.

## Do Not

- Add singular rule roots, separate parameter/test fixture files, or generated formula artifacts.
- Put unrelated jurisdiction materials here.
- Add source text, PDFs, HTML snapshots, generated source payloads, or Python bytecode to Git.
