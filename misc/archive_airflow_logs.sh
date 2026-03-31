#!/usr/bin/env bash
set -euo pipefail

# year=2025; for dag_id in dag1 dag2; do for instrument in test1 test2; do for month in 01 02 03 04 05 06 07 08 09 10 11 12; do echo $year $month $dag_id $instrument; ./archive_airflow_logs.sh . $year $month $dag_id.$instrument --delete; done; done; done

usage() {
    echo "Usage: $0 LOG_BASE YEAR MONTH DAG_ID [--delete]"
    echo ""
    echo "Archive airflow logs for a specific dag and month into a .tar.gz file."
    echo ""
    echo "  LOG_BASE  Path to the airflow_logs directory"
    echo "  YEAR      4-digit year (e.g. 2026)"
    echo "  MONTH     2-digit month (01-12)"
    echo "  DAG_ID    Name of the DAG"
    echo "  --delete  Remove archived run_id directories after successful tar"
    echo "available DAG IDs:"
    ls -d dag_id=* | sed 's/dag_id=//' | cut -d. -f1 | sort -u | tr '\n' ' '
    echo
    echo "available instruments:"
    ls -d dag_id=* | sed 's/dag_id=//' | cut -d. -f2 | grep -v '^$' | sort -u | tr '\n' ' '
    echo
    exit 1
}

if [[ $# -lt 4 || $# -gt 5 ]]; then
    usage
fi

LOG_BASE="$1"
YEAR="$2"
MONTH="$3"
DAG_ID="$4"
DELETE=false

if [[ $# -eq 5 ]]; then
    if [[ "$5" == "--delete" ]]; then
        DELETE=true
    else
        echo "Error: Unknown option '$5'"
        usage
    fi
fi

if [[ ! -d "$LOG_BASE" ]]; then
    echo "Error: LOG_BASE directory does not exist: $LOG_BASE"
    exit 1
fi

# Validate year
if ! [[ "$YEAR" =~ ^[0-9]{4}$ ]]; then
    echo "Error: YEAR must be a 4-digit number, got '$YEAR'"
    exit 1
fi

# Validate month
if ! [[ "$MONTH" =~ ^(0[1-9]|1[0-2])$ ]]; then
    echo "Error: MONTH must be 01-12, got '$MONTH'"
    exit 1
fi

DAG_DIR="$LOG_BASE/dag_id=$DAG_ID"

if [[ ! -d "$DAG_DIR" ]]; then
    echo "Error: DAG directory does not exist: $DAG_DIR"
    exit 1
fi

# Find matching run_id directories (both manual__ and scheduled__ prefixes)
echo Collecting dirs ..
matching_dirs=()
for dir in "$DAG_DIR"/run_id=*__"${YEAR}-${MONTH}"-*; do
    if [[ -d "$dir" ]]; then
        matching_dirs+=("$(basename "$dir")")
        echo $dir
    fi
done

if [[ ${#matching_dirs[@]} -eq 0 ]]; then
    echo "No run_id directories found for ${YEAR}-${MONTH} in $DAG_DIR"
    exit 0
fi

# Prepare archive
ARCHIVE_DIR="$LOG_BASE/archive"
mkdir -p "$ARCHIVE_DIR"

ARCHIVE_FILE="$ARCHIVE_DIR/${DAG_ID}__${YEAR}-${MONTH}.tar.gz"

if [[ -f "$ARCHIVE_FILE" ]]; then
    echo "Error: Archive already exists: $ARCHIVE_FILE"
    exit 1
fi

# Create tar archive with paths relative to the dag_id directory
echo Creating archive ..
tar -czvf "$ARCHIVE_FILE" -C "$DAG_DIR" "${matching_dirs[@]}"

# checks
echo Checking archive ..
gzip -t $ARCHIVE_FILE
#tar -tzf $ARCHIVE_FILE > /dev/null

ARCHIVE_SIZE=$(ls -lh "$ARCHIVE_FILE" | awk '{print $5}')
echo "Archived ${#matching_dirs[@]} run_id directories into $ARCHIVE_FILE ($ARCHIVE_SIZE)"

# Delete originals if requested
if [[ "$DELETE" == true ]]; then
    for dir in "${matching_dirs[@]}"; do
        rm -rf "$DAG_DIR/$dir"
    done
    echo "Deleted ${#matching_dirs[@]} archived run_id directories"
fi
