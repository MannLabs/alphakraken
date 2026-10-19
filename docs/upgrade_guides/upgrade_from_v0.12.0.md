# Upgrade from v0.12.0

## Backup location per raw file

`RawFile.backup_display_base_path` records `display_paths.backup` at copy time, and is used to display the backup path.
Hence, change `display_paths.backup` only when the backup file system changes.

After deploying, backfill existing raw files with the current `display_paths.backup`:
```
PYTHONPATH=. python shared/_migrations/from_0.12.0/_backfill_backup_display_base_path.py --backup-display-base-path <display_paths.backup> --dry-run
```

## Output location per metrics

Likewise, `Metrics.output_display_base_path` records `display_paths.output` when the metrics are written.
Change `display_paths.output` only when the output file system changes.

Backfill existing metrics with the current `display_paths.output`:
```
PYTHONPATH=. python shared/_migrations/from_0.12.0/_backfill_output_display_base_path.py --output-display-base-path <display_paths.output> --dry-run
```
