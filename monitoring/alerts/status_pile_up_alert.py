"""Status pile-up alert checker."""

from shared.db.models import (
    TERMINAL_STATUSES,
    KrakenStatus,
    KrakenStatusEntities,
    RawFile,
)

from .base_alert import BaseAlert, Issue
from .config import STATUS_PILE_UP_THRESHOLDS


class StatusPileUpAlert(BaseAlert):
    """Check for status pile-ups (too many files in non-terminal statuses)."""

    def _get_issues(self, status_objects: list[KrakenStatus]) -> list[Issue[str]]:
        """Check for status pile-ups on instruments."""
        status_pile_up_instruments = []

        for kraken_status in [
            o
            for o in status_objects
            if o.entity_type == KrakenStatusEntities.INSTRUMENT
        ]:
            instrument_id = kraken_status.id
            status_counts = RawFile.objects(
                instrument_id=instrument_id, status__nin=TERMINAL_STATUSES
            ).item_frequencies("status")

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

    def format_message(self, issues: list[Issue[str]]) -> str:
        """Format status pile-up message."""
        instruments_str = "\n".join(
            [f"- `{issue.identifier}`: {issue.details}" for issue in issues]
        )
        return f"Status pile up detected:\n{instruments_str}"
