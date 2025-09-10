"""Status pile-up alert checker."""

import config
from config import Cases

from shared.db.models import (
    TERMINAL_STATUSES,
    KrakenStatus,
    KrakenStatusEntities,
    RawFile,
)

from .base_alert import BaseAlert


class StatusPileUpAlert(BaseAlert):
    """Check for status pile-ups (too many files in non-terminal statuses)."""

    @property
    def case_name(self) -> str:
        """Return the case name for this alert type."""
        return Cases.STATUS_PILE_UP

    def check(self, status_objects: list[KrakenStatus]) -> list[tuple[str, str]]:
        """Check for status pile-ups on instruments."""
        status_pile_up_instruments = []

        for kraken_status in status_objects:
            # Only check instruments
            if kraken_status.entity_type != KrakenStatusEntities.INSTRUMENT:
                continue

            instrument_id = kraken_status.id
            status_counts = RawFile.objects(
                instrument_id=instrument_id, status__nin=TERMINAL_STATUSES
            ).item_frequencies("status")

            piled_up_statuses = [
                status
                for status, count in status_counts.items()
                if count > config.STATUS_PILE_UP_THRESHOLDS[status]
            ]

            if piled_up_statuses:
                piled_up_statuses_str = "; ".join(
                    [
                        f"{status}: {status_counts[status]}"
                        for status in piled_up_statuses
                    ]
                )
                status_pile_up_instruments.append(
                    (instrument_id, piled_up_statuses_str)
                )

        return status_pile_up_instruments

    def format_message(self, issues: list[tuple[str, str]]) -> str:
        """Format status pile-up message."""
        instruments_str = "\n".join(
            [
                f"- `{instrument_id}`: {status_info}"
                for instrument_id, status_info in issues
            ]
        )
        return f"Status pile up detected:\n{instruments_str}"
