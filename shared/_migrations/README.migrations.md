# Migrations

One-off scripts for migrating/backfilling the database between releases.

- Subfolders (`from_<version>/`) group migrations by the release they upgrade from.
- Each script documents its purpose and usage in its module docstring.
- Most support `--dry-run`; run it first.
- Make a copy of the database before applying any migration!

Run with DB credentials exported as env vars, e.g.:

```
set -a; source envs/sandbox.env ; set +a;
PYTHONPATH=. python shared/_migrations/from_0.8.0/_backfill_fallback_project_id.py --dry-run
```
