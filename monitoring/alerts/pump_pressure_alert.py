"""Pump pressure increase alert checker."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

import pytz
from db.interface import augment_raw_files_with_metrics

from shared.db.models import KrakenStatus, RawFile

from .base_alert import BaseAlert
from .config import (
    PUMP_PRESSURE_LOOKBACK_DAYS,
    PUMP_PRESSURE_WINDOW_SIZE,
    Cases,
)


class PumpPressureAlert(BaseAlert):
    """Check for pump pressure increases across instruments."""

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

        # Group metrics by instrument
        instrument_data = self._group_metrics_by_instrument(raw_files_with_metrics)

        # Check each instrument separately
        issues = []
        for instrument_id, pressure_data in instrument_data.items():
            if len(pressure_data) < PUMP_PRESSURE_WINDOW_SIZE + 1:
                logging.debug(
                    f"Not enough data points for {instrument_id}: "
                    f"{len(pressure_data)} < {PUMP_PRESSURE_WINDOW_SIZE + 1}"
                )
                continue

            is_alert, pressures, pressure_changes = self._detect_pressure_increase(
                pressure_data, 5, 20
            )

            if is_alert:
                issues.append(
                    (
                        instrument_id,
                        f"Pressure data: {pressures}; Pressure changes: {pressure_changes}",
                    )
                )

        return issues

    def _group_metrics_by_instrument(
        self, raw_files_with_metrics: dict[str, Any]
    ) -> dict[str, list[tuple[float, float, datetime]]]:
        """Group metrics by instrument, flatten to a tuple (pump_pressure, gradient_length, created_at,)."""
        ii = defaultdict(list)
        for v in raw_files_with_metrics.values():
            gl = v.get("metrics_alphadia", {}).get("raw:gradient_length_m")
            pp = v.get("metrics_msqc", {}).get("msqc_evosep_pump_hp_pressure_max")

            if gl is not None and pp is not None:
                ii[v["instrument_id"]].append((pp, gl, v["created_at"]))

        return ii

    def _detect_pressure_increase(
        self,
        pressure_data: list[tuple[float, float, datetime]],
        window_size: int,
        threshold: float,
    ) -> tuple[bool, list[float], list[float]]:
        """Detect if pressure increases by more than threshold over the last window_size samples.

        Args:
            pressure_data: pandas Series or array of pressure values
            window_size: number of past samples to look at
            threshold: pressure increase threshold to trigger alert

        Returns:
            alerts: boolean array indicating where alerts would be triggered
            pressure_changes: array of pressure changes over the window

        """
        alerts = []
        pressure_changes = []
        pressures = []
        latest_gradient_length = pressure_data[0][1]

        pressure_data = sorted(
            pressure_data, reverse=False, key=lambda x: x[2]
        )  # sort 'oldest first'

        for i in range(len(pressure_data)):
            if (
                not 0.9  # noqa: PLR2004
                < (pressure_data[i - window_size][1] / latest_gradient_length)
                < 1.1  # noqa: PLR2004
            ):
                continue

            pressures.append(pressure_data[i][0])
            if i >= window_size:
                # Calculate pressure change over the window
                current_pressure = pressure_data[i][0]
                past_pressure = pressure_data[i - window_size][0]
                pressure_change = current_pressure - past_pressure

                pressure_changes.append(pressure_change)
                alerts.append(pressure_change > threshold)

        return any(alerts), pressures, pressure_changes

    def format_message(self, issues: list[tuple[str, str]]) -> str:
        """Format pump pressure alert message."""
        instruments_str = "\n".join(
            [f"- `{instrument_id}`: {details}" for instrument_id, details in issues]
        )
        return f"Pump pressure increase detected:\n{instruments_str}"
