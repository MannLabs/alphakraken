#!/bin/bash

# Bounds the demo's disk usage by keeping only the KEEP_LAST newest raw files in the pool backup,
# the quanting output and the instrument backup folder.
#
# On top of that, a pool backup entry is kept for as long as the instrument backup folder still
# holds a file of the same name, so the pool copy never disappears while the instrument copy is
# still around - the remover checks the pool backup before it deletes from the instrument.
#
# Keeping the newest few rather than wiping everything is what makes this safe to run at any time:
# at a 21-minute cadence one file is always being monitored while the previous one is being copied,
# checksummed or quanted, and a wipe would take that data away mid-flight. The MongoDB records are
# deliberately left alone, so the metrics history in the webapp keeps growing while the disk does not.
#
# Meant to be run hourly from cron, cf. misc/demo/README.md.

set -e -u

DEMO_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

MOUNTS_DIR=${MOUNTS_DIR:-${DEMO_DIR}/mounts}
INSTRUMENT=${INSTRUMENT:-demo1}
KEEP_LAST=${KEEP_LAST:-2}

# Deletes all but the KEEP_LAST newest entries at exactly $2 levels below $1. If $3 is given, an
# entry is kept as long as a file or folder of the same name still exists in that folder.
# Raw file names cannot contain spaces (cf. shared/validation.py), so line-based handling is safe.
prune_tree() {
  local root=$1
  local depth=$2
  local guard_dir=${3:-}

  if [ ! -d "$root" ]; then
    return
  fi

  local entry
  while read -r entry; do
    # only ever delete below the tree we were asked to prune
    case "$entry" in
      "$root"/*) ;;
      *) continue ;;
    esac
    if [ -n "$guard_dir" ] && [ -e "${guard_dir}/$(basename "$entry")" ]; then
      echo "  keeping ${entry#"${MOUNTS_DIR}"/}, still on the instrument"
      continue
    fi
    echo "  removing ${entry#"${MOUNTS_DIR}"/}"
    rm -rf "$entry"
  done < <(
    # 'ls -dt' sorts the paths handed to it by modification time, newest first. With more entries
    # than fit in one argument list find would sort them in batches, which only matters if this has
    # not run for many months (17k entries per batch).
    find "$root" -mindepth "$depth" -maxdepth "$depth" -exec ls -dt {} + |
      tail -n "+$((KEEP_LAST + 1))"
  )

  # the <year_month> / <project_id> parents left behind by the deletions above
  if [ "$depth" -gt 1 ]; then
    find "$root" -mindepth 1 -maxdepth $((depth - 1)) -type d -empty -delete
  fi
}

echo "Pruning demo data in ${MOUNTS_DIR}, keeping the ${KEEP_LAST} newest of each:"

INSTRUMENT_BACKUP_DIR=${MOUNTS_DIR}/instruments/${INSTRUMENT}/Backup

# A pool backup entry is only dropped once the instrument no longer holds it, even if it is older
# than the KEEP_LAST newest: that is the invariant the remover relies on, and it is why the pool
# backup is pruned *before* the instrument backup - the other order would delete both copies of the
# same file in one run, and the guard would never bite.
echo "pool backup:"
prune_tree "${MOUNTS_DIR}/backup/${INSTRUMENT}" 2 "$INSTRUMENT_BACKUP_DIR"

echo "quanting output:"
prune_tree "${MOUNTS_DIR}/output" 2

echo "instrument backup:"
prune_tree "$INSTRUMENT_BACKUP_DIR" 1

echo "done."
