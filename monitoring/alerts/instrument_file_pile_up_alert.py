"""Instrument file pile-up alert checker."""

from shared.db.interface import get_raw_files_by_instrument_file_status
from shared.db.models import KrakenStatus, KrakenStatusEntities

from .base_alert import BaseAlert, Issue
from .config import (
    INSTRUMENT_FILE_MIN_AGE_HOURS,
    INSTRUMENT_FILE_PILE_UP_THRESHOLDS,
)


class InstrumentFilePileUpAlert(BaseAlert):
    """Check for instrument file pile-ups (files not moved/purged)."""

    title = "Instrument file pile up detected (files not moved/purged)"

    def _get_issues(self, status_objects: list[KrakenStatus]) -> list[Issue[str]]:
        """Check for instrument file pile-ups."""
        instrument_file_pile_up_instruments = []

        for kraken_status in [
            o
            for o in status_objects
            if o.entity_type == KrakenStatusEntities.INSTRUMENT
        ]:
            instrument_id = kraken_status.id
            pile_up_detected = []

            for (
                instrument_file_status,
                threshold,
            ) in INSTRUMENT_FILE_PILE_UP_THRESHOLDS.items():
                files = get_raw_files_by_instrument_file_status(
                    instrument_file_status,
                    instrument_id=instrument_id,
                    min_age_hours=INSTRUMENT_FILE_MIN_AGE_HOURS,
                )
                count = len(files)

                if count > threshold:
                    pile_up_detected.append(f"{instrument_file_status}: {count}")

            if pile_up_detected:
                pile_up_str = "; ".join(pile_up_detected)
                instrument_file_pile_up_instruments.append(
                    Issue(instrument_id, pile_up_str, instrument_id=instrument_id)
                )

        return instrument_file_pile_up_instruments
