# The duplicate rule of settings assignments is stricter than its purpose

2026-11-19, found while seeding the e2e stack with a production sample (branch `rest_api_writes`, cca28b4f).

## Finding

Production has the same settings assigned twice to one project, with the same file name filters
and disjoint scopes:

| Project | Settings | Scopes | Excluded scopes |
|---|---|---|---|
| `ADIAMA` | `fallback_v8` | `sciex` | |
| `ADIAMA` | `fallback_v8` | `thermo` | `stellar1` |
| `_FALLBACK` | `fallback_v8` | `thermo` | `stellar1` |
| `_FALLBACK` | `fallback_v8` | `sciex` | |

The webapp shows them as a sensible configuration. The rule in `shared/db/project_settings.py`
`_duplicate_errors` rejects it: same settings and same file name filters is a duplicate,
whatever the scopes.

## Cause

- Before v0.10.0, an assignment had one `scope`. The migration
  `shared/_migrations/from_0.10.0/_migrate_project_settings_scopes.py` converted every assignment
  one to one, without merging.
- The duplicate rule came with the same change (`078fe6e3`, in `main` since 2026-09-15) and
  ignores the scopes. So migrated data is invalid under rules of the same release.

## What the rule protects

Two assignments of the same settings that match the same raw file write to the same output
folder, because the folder is named after the settings (`out_<raw file>/<settings>_v<version>`).
That collision needs both assignments to match one raw file. With disjoint scopes, as above,
no raw file matches both.

## Effects

1. The setup cannot be created again, neither in the webapp nor in the REST API; only as one
   merged assignment (scopes `[thermo, sciex]`, excluded `[stellar1]`), which is exact here
   because `stellar1` is a thermo instrument.
2. `update_to_latest` works for the first of the two and then fails for the second with the
   duplicate error (e.g. pressing "↑" on both rows after a `fallback_v9` exists).
3. Merging is not exact in general: excluded scopes apply to the whole merged assignment, so an
   exclusion meant for one scope can remove instruments of the other.

## Options

1. **The rule checks overlap.** Reject only if the two assignments can match the same raw file:
   overlapping instruments after the exclusions, and file name filters that can match the same
   file. Production data becomes valid, `update_to_latest` works for both. A `shared.db`
   change; the webapp and the REST API inherit it.
2. **Merge in a migration.** Keep the rule; merge duplicates where that is exact, report the
   rest for manual fixing.
3. **Leave as is.** A known issue; admins unlink and re-assign merged entries by hand.

Recommendation: 1, because the rule's purpose is the output folder collision, and option 1
states exactly that.

## Relevance for the REST API work

- `POST /assignments/` and `POST /assignments/{id}/update_to_latest` return the duplicate
  error (422) in the cases above.
- The e2e seed skips the second assignment of each pair when seeding the production sample.
