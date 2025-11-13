"""Pump pressure increase alert checker."""

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pytz

from shared.db.interface import augment_raw_files_with_metrics
from shared.db.models import KrakenStatus, RawFile

from .base_alert import BaseAlert
from .config import (
    BUSINESS_ALERTS_WEBHOOK_URL,
    PUMP_PRESSURE_ABSOLUTE_THRESHOLD_BAR,
    PUMP_PRESSURE_LOOKBACK_DAYS,
    PUMP_PRESSURE_THRESHOLD_BAR,
    PUMP_PRESSURE_WINDOW_SIZE,
    Cases,
)

# temporary hack to bypass bug in pump pressure extraction
MAX_PRESSURE_TO_CONSIDER = 1000


@dataclass(frozen=True)
class PressureDataPoint:
    """Represents a single pressure measurement with associated metadata."""

    pressure: float
    gradient_length: float
    created_at: datetime
    raw_file_id: str


class PumpPressureAlert(BaseAlert):
    """Check for pump pressure increases across instruments."""

    def __init__(self) -> None:
        """Initialize the alert with memory for tracking reported issues."""
        super().__init__()
        # Memory: set of (instrument_id, tuple of pressure_changes) to track reported issues
        self._reported_issues: set[tuple[str, tuple[Any]]] = set()

    @property
    def name(self) -> str:
        """Return the case name for this alert type."""
        return Cases.PUMP_PRESSURE_INCREASE

    def get_webhook_url(self) -> str:
        """Return the configured BUSINESS_ALERTS_WEBHOOK_URL for pump pressure alerts."""
        return BUSINESS_ALERTS_WEBHOOK_URL

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
            # Check for pressure increase (relative threshold)
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
                            f"Pressure changes: {self._format(pressure_changes)}",
                        )
                    )
                else:
                    logging.debug(
                        f"Suppressing duplicate alert for {instrument_id} "
                        f"with pressure changes: {pressure_changes}"
                    )

            # Check for high absolute pressure
            high_pressure_alerts = self._detect_high_absolute_pressure(
                pressure_data, PUMP_PRESSURE_ABSOLUTE_THRESHOLD_BAR
            )
            for pressure, raw_file_id, timestamp in high_pressure_alerts:
                memory_key = (instrument_id, f"absolute_{raw_file_id}")

                if memory_key not in self._reported_issues:
                    self._reported_issues.add(memory_key)
                    issues.append(
                        (
                            instrument_id,
                            f"High pressure: {pressure:.1f} bar (≥{PUMP_PRESSURE_ABSOLUTE_THRESHOLD_BAR} bar) in `{raw_file_id}` at {timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
                        )
                    )
                else:
                    logging.debug(
                        f"Suppressing duplicate absolute pressure alert for {instrument_id} "
                        f"with raw_file_id: {raw_file_id}"
                    )

        return issues

    @staticmethod
    def _format(changes: list[tuple[float, float, float, datetime, str, str]]) -> str:
        """Format pressure changes for alert message."""
        return "; ".join(
            [
                f"+{change[0]:.1f} bar (from {change[2]:.1f} bar in `{change[5]}` to {change[1]:.1f} bar in `{change[4]}` at {change[3].strftime('%Y-%m-%d %H:%M:%S')})"
                for change in changes
            ]
        )

    def _get_pressure_data_by_instrument(
        self, raw_files_with_metrics: dict[str, Any]
    ) -> dict[str, list[PressureDataPoint]]:
        """Group metrics by instrument, returning PressureDataPoint instances."""
        pressure_data = defaultdict(list)
        for raw_file_id, v in raw_files_with_metrics.items():
            gradient_length = v.get("metrics_alphadia", {}).get("raw:gradient_length_m")
            pressure = v.get("metrics_msqc", {}).get("msqc_evosep_pump_hp_pressure_max")

            if gradient_length is not None and pressure is not None:
                pressure_data[v["instrument_id"]].append(
                    PressureDataPoint(
                        pressure, gradient_length, v["created_at"], raw_file_id
                    )
                )

        return pressure_data

    def _detect_pressure_increase(
        self,
        pressure_data: list[PressureDataPoint],
        window_size: int,
        threshold: float,
    ) -> tuple[bool, list[tuple[float, float, float, datetime, str, str]]]:
        """Detect if pressure increases by more than threshold over the last window_size samples.

        Args:
            pressure_data: list of PressureDataPoint instances, ordered newest first
            window_size: number of past samples to look at
            threshold: pressure increase threshold to trigger alert

        Returns:
            is_alert: boolean flag indicating if alerts were detected
            pressure_changes: list of tuples (pressure_change, current_pressure, past_pressure,
                             timestamp, younger_file_name, older_file_name)

        """
        latest_gradient_length = pressure_data[0].gradient_length

        # logging.info(f"pressure_data: {pressure_data}")

        def _is_within_tolerance(
            value: float,
            target: float,
            tolerance: float = 0.1,
        ) -> bool:
            """Check if value is within relative tolerance of target (default +-10%)."""
            return (1 - tolerance) < (value / target) < (1 + tolerance)

        is_alert = False
        pressure_changes = []

        for i in range(len(pressure_data)):
            if i < window_size:
                continue

            data_younger = pressure_data[i - window_size]
            data_older = pressure_data[i]

            if not _is_within_tolerance(
                data_older.gradient_length, latest_gradient_length
            ) or not _is_within_tolerance(
                data_younger.gradient_length, latest_gradient_length
            ):
                continue

            # Calculate pressure change over the window
            current_pressure = data_younger.pressure
            past_pressure = data_older.pressure
            pressure_change = current_pressure - past_pressure

            if (
                past_pressure > MAX_PRESSURE_TO_CONSIDER
                or current_pressure > MAX_PRESSURE_TO_CONSIDER
            ):
                continue

            if pressure_change > threshold:
                pressure_changes.append(
                    (
                        pressure_change,
                        current_pressure,
                        past_pressure,
                        data_younger.created_at,
                        data_younger.raw_file_id,
                        data_older.raw_file_id,
                    )
                )
                is_alert = True
                break

        return is_alert, pressure_changes

    def _detect_high_absolute_pressure(
        self,
        pressure_data: list[PressureDataPoint],
        threshold: float,
    ) -> list[tuple[float, str, datetime]]:
        """Detect if any pressure measurements exceed the absolute threshold.

        Args:
            pressure_data: list of PressureDataPoint instances
            threshold: absolute pressure threshold to trigger alert

        Returns:
            List of tuples (pressure, raw_file_id, timestamp) for measurements >= threshold

        """
        high_pressure_measurements = []

        for data_point in pressure_data:
            if data_point.pressure > MAX_PRESSURE_TO_CONSIDER:
                continue

            if data_point.pressure >= threshold:
                high_pressure_measurements.append(
                    (data_point.pressure, data_point.raw_file_id, data_point.created_at)
                )

        return high_pressure_measurements

    def format_message(self, issues: list[tuple[str, str]]) -> str:
        """Format pump pressure alert message."""
        instruments_str = "\n".join(
            [f"- `{instrument_id}`: {details}" for instrument_id, details in issues]
        )
        return f"Pump pressure increase detected:\n{instruments_str}"
