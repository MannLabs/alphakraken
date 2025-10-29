"""Pump pressure increase alert checker."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

import pytz

from shared.db.interface import augment_raw_files_with_metrics
from shared.db.models import KrakenStatus, RawFile

from .base_alert import BaseAlert
from .config import (
    PUMP_PRESSURE_LOOKBACK_DAYS,
    PUMP_PRESSURE_THRESHOLD_BAR,
    PUMP_PRESSURE_WINDOW_SIZE,
    Cases,
)


class PumpPressureAlert(BaseAlert):
    """Check for pump pressure increases across instruments."""

    def __init__(self) -> None:
        """Initialize the alert with memory for tracking reported issues."""
        super().__init__()
        # Memory: set of (instrument_id, tuple of pressure_changes) to track reported issues
        self._reported_issues: set[tuple[str, tuple[float, ...]]] = set()

    @property
    def name(self) -> str:
        """Return the case name for this alert type."""
        return Cases.PUMP_PRESSURE_INCREASE

    def _get_issues(self, status_objects: list[KrakenStatus]) -> list[tuple[str, str]]:
        """Check for pump pressure increases per instrument."""
        # Get all instrument IDs
        instrument_ids = [
            status.id for status in status_objects if status.entity_type == "instrument"
        ]

        if not instrument_ids:
            logging.debug("No instruments found in status objects")
            return []

        # Query recent metrics for all instruments
        cutoff = datetime.now(tz=pytz.utc) - timedelta(days=PUMP_PRESSURE_LOOKBACK_DAYS)

        # Get raw files for all instruments in the time window
        raw_files = (
            RawFile.objects.filter(
                instrument_id__in=instrument_ids, created_at__gte=cutoff
            )
            .only("id", "instrument_id", "created_at")
            .order_by("-created_at")
        )

        if not raw_files:
            logging.debug("No raw files found in lookback window")
            return []

        raw_files_with_metrics = augment_raw_files_with_metrics(
            raw_files, ["raw:gradient_length_m", "msqc_evosep_pump_hp_pressure_max"]
        )

        instrument_data = self._get_pressure_data_by_instrument(raw_files_with_metrics)

        issues = []
        for instrument_id, pressure_data in instrument_data.items():
            # if len(pressure_data) < PUMP_PRESSURE_WINDOW_SIZE + 1:
            #     logging.debug(
            #         f"Not enough data points for {instrument_id}: "
            #         f"{len(pressure_data)} < {PUMP_PRESSURE_WINDOW_SIZE + 1}"
            #     )
            #     continue

            is_alert, pressure_changes = self._detect_pressure_increase(
                pressure_data, PUMP_PRESSURE_WINDOW_SIZE, PUMP_PRESSURE_THRESHOLD_BAR
            )

            if is_alert:
                # Create memory key from instrument_id and pressure changes
                memory_key = (instrument_id, tuple(pressure_changes))

                # Only report if not already reported
                if memory_key not in self._reported_issues:
                    self._reported_issues.add(memory_key)
                    issues.append(
                        (
                            instrument_id,
                            f"Pressure changes: {pressure_changes}",
                        )
                    )
                else:
                    logging.debug(
                        f"Suppressing duplicate alert for {instrument_id} "
                        f"with pressure changes: {pressure_changes}"
                    )

        return issues

    def _get_pressure_data_by_instrument(
        self, raw_files_with_metrics: dict[str, Any]
    ) -> dict[str, list[tuple[float, float, datetime]]]:
        """Group metrics by instrument, flatten to a tuple (pump_pressure, gradient_length, created_at,)."""
        pressure_data = defaultdict(list)
        for v in raw_files_with_metrics.values():
            gradient_length = v.get("metrics_alphadia", {}).get("raw:gradient_length_m")
            pressure = v.get("metrics_msqc", {}).get("msqc_evosep_pump_hp_pressure_max")

            if gradient_length is not None and pressure is not None:
                pressure_data[v["instrument_id"]].append(
                    (pressure, gradient_length, v["created_at"])
                )

        return pressure_data

    def _detect_pressure_increase(
        self,
        pressure_data: list[tuple[float, float, datetime]],
        window_size: int,
        threshold: float,
    ) -> tuple[bool, list[tuple[float, float, float, datetime]]]:
        """Detect if pressure increases by more than threshold over the last window_size samples.

        Args:
            pressure_data: list of (pressure, gradient_length, created_at) tuples, ordered newest first
            window_size: number of past samples to look at
            threshold: pressure increase threshold to trigger alert

        Returns:
            is_alert: boolean flag indicating if alerts were detected
            pressure_changes: list of pressure changes over the window

        """
        latest_gradient_length = pressure_data[0][1]

        # logging.info(f"pressure_data: {pressure_data}")

        def _within_pressure_tolerance(
            value: float,
            target: float,
            tolerance: float = 10,
        ) -> bool:
            """Check if value is within relative tolerance of target."""
            return (1 - tolerance) < (value / target) < (1 + tolerance)

        is_alert = False
        pressure_changes = []

        for i in range(len(pressure_data)):
            if i < window_size:
                continue

            data_younger = pressure_data[i - window_size]
            data_older = pressure_data[i]

            if not _within_pressure_tolerance(
                data_older[1], latest_gradient_length
            ) or not _within_pressure_tolerance(
                data_younger[1], latest_gradient_length
            ):
                continue

            # Calculate pressure change over the window
            current_pressure = data_younger[0]
            past_pressure = data_older[0]
            pressure_change = current_pressure - past_pressure

            if pressure_change > threshold:
                pressure_changes.append(
                    (pressure_change, current_pressure, past_pressure, data_younger[2])
                )
                is_alert = True
                break

        return is_alert, pressure_changes

    def format_message(self, issues: list[tuple[str, str]]) -> str:
        """Format pump pressure alert message."""
        instruments_str = "\n".join(
            [f"- `{instrument_id}`: {details}" for instrument_id, details in issues]
        )
        return f"Pump pressure increase detected:\n{instruments_str}"
