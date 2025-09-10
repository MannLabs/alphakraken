"""Instrument file pile-up alert checker."""

import config
from config import Cases

from shared.db.interface import get_raw_files_by_instrument_file_status
from shared.db.models import KrakenStatus, KrakenStatusEntities

from .base_alert import BaseAlert


class InstrumentFilePileUpAlert(BaseAlert):
    """Check for instrument file pile-ups (files not moved/purged)."""

    @property
    def case_name(self) -> str:
        """Return the case name for this alert type."""
        return Cases.INSTRUMENT_FILE_PILE_UP

    def get_issues(self, status_objects: list[KrakenStatus]) -> list[tuple[str, str]]:
        """Check for instrument file pile-ups."""
        instrument_file_pile_up_instruments = []

        for kraken_status in status_objects:
            # Only check instruments
            if kraken_status.entity_type != KrakenStatusEntities.INSTRUMENT:
                continue

            instrument_id = kraken_status.id
            pile_up_detected = []

            for (
                instrument_file_status,
                threshold,
            ) in config.INSTRUMENT_FILE_PILE_UP_THRESHOLDS.items():
                files = get_raw_files_by_instrument_file_status(
                    instrument_file_status,
                    instrument_id=instrument_id,
                    min_age_hours=config.INSTRUMENT_FILE_MIN_AGE_HOURS,
                )
                count = len(files)

                if count > threshold:
                    pile_up_detected.append(f"{instrument_file_status}: {count}")

            if pile_up_detected:
                pile_up_str = "; ".join(pile_up_detected)
                instrument_file_pile_up_instruments.append((instrument_id, pile_up_str))

        return instrument_file_pile_up_instruments

    def format_message(self, issues: list[tuple[str, str]]) -> str:
        """Format instrument file pile-up message."""
        instruments_str = "\n".join(
            [f"- `{instrument_id}`: {file_info}" for instrument_id, file_info in issues]
        )
        return f"Instrument file pile up detected (files not moved/purged):\n{instruments_str}"
