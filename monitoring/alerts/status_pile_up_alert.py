"""Status pile-up alert checker."""

from datetime import datetime

from shared.db import raw_files
from shared.domain.raw_files import TERMINAL_STATUSES
from shared.domain.status import KrakenStatus, KrakenStatusEntities

from .base_alert import BaseAlert, Issue
from .config import STATUS_PILE_UP_THRESHOLDS


class StatusPileUpAlert(BaseAlert):
    """Check for status pile-ups (too many files in non-terminal statuses)."""

    title = "Status pile up detected"

    def _get_issues(
        self, status_objects: list[KrakenStatus], now: datetime
    ) -> list[Issue[str]]:
        """Check for status pile-ups on instruments."""
        del now  # unused
        status_pile_up_instruments = []

        for kraken_status in [
            o
            for o in status_objects
            if o.entity_type == KrakenStatusEntities.INSTRUMENT
        ]:
            instrument_id = kraken_status.id
            status_counts = raw_files.count_by_status(
                raw_files.RawFileFilter(
                    instrument_id=instrument_id, statuses_not=TERMINAL_STATUSES
                )
            )

            piled_up_statuses = [
                status
                for status, count in status_counts.items()
                if count > STATUS_PILE_UP_THRESHOLDS[status]
            ]

            if piled_up_statuses:
                piled_up_statuses_str = "; ".join(
                    [
                        f"{status}: {status_counts[status]}"
                        for status in piled_up_statuses
                    ]
                )
                status_pile_up_instruments.append(
                    Issue(
                        instrument_id,
                        piled_up_statuses_str,
                        instrument_id=instrument_id,
                    )
                )

        return status_pile_up_instruments
