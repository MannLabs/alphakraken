"""Pump pressure alerts: significant increases over a short period, and pressures above an absolute threshold."""

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from shared.db.interface import augment_raw_files_with_metrics
from shared.db.models import KrakenStatus, KrakenStatusEntities, RawFile

from .base_alert import BaseAlert, Issue
from .config import (
    PUMP_PRESSURE_ABSOLUTE_THRESHOLD_BAR,
    PUMP_PRESSURE_GRADIENT_TOLERANCE,
    PUMP_PRESSURE_LOOKBACK_DAYS,
    PUMP_PRESSURE_THRESHOLD_BAR,
    PUMP_PRESSURE_WINDOW_SIZE,
    Destinations,
)

# temporary hack to bypass bug in pump pressure extraction
MAX_PRESSURE_TO_CONSIDER = 1000


@dataclass(frozen=True)
class PressureDataPoint:
    """Represents a single pressure measurement with associated metadata."""

    pressure: float
    gradient_length: float | None
    created_at: datetime
    raw_file_id: str


@dataclass(frozen=True)
class PressureChange:
    """A pressure increase between two measurements of comparable gradient length."""

    younger: PressureDataPoint
    older: PressureDataPoint

    @property
    def increase(self) -> float:
        """Return the pressure difference, positive for an increase."""
        return self.younger.pressure - self.older.pressure


def load_pressure_data(
    instrument_ids: list[str], now: datetime
) -> dict[str, list[PressureDataPoint]]:
    """Load the pressure measurements of the recent raw files per instrument, newest first."""
    cutoff = now - timedelta(days=PUMP_PRESSURE_LOOKBACK_DAYS)
    raw_files = (
        RawFile.objects.filter(instrument_id__in=instrument_ids, created_at__gte=cutoff)
        .only("id", "instrument_id", "created_at")
        .order_by("-created_at")
    )
    if not raw_files:
        logging.debug("No raw files found in lookback window")
        return {}

    # TODO: centralize column names and harmonization
    raw_files_with_metrics = augment_raw_files_with_metrics(
        raw_files,
        [
            "raw:gradient_length_m",  # alphadia < 2
            "gradient_length",
            "msqc_evosep_pump_hp_pressure_max",  # older alphakraken
            "evosep_pump_hp_pressure_max",
        ],
    )
    return group_by_instrument(raw_files_with_metrics)


def group_by_instrument(
    raw_files_with_metrics: dict[str, Any],
) -> dict[str, list[PressureDataPoint]]:
    """Group the metrics by instrument, skipping raw files without pressure."""
    pressure_data = defaultdict(list)
    for raw_file_id, v in raw_files_with_metrics.items():
        gradient_length = v.get("metrics_alphadia", {}).get("gradient_length")
        pressure = v.get("metrics_msqc", {}).get("evosep_pump_hp_pressure_max")

        if pressure is not None:
            pressure_data[v["instrument_id"]].append(
                PressureDataPoint(
                    pressure, gradient_length, v["created_at"], raw_file_id
                )
            )

    return pressure_data


def is_within_tolerance(
    value: float | None,
    target: float | None,
    tolerance: float = PUMP_PRESSURE_GRADIENT_TOLERANCE,
) -> bool:
    """Check if value is within relative tolerance of target, False if either is None."""
    if value is None or target is None:
        return False
    return (1 - tolerance) < (value / target) < (1 + tolerance)


def detect_increase(
    points: list[PressureDataPoint], window_size: int, threshold: float
) -> PressureChange | None:
    """Return the first increase above the threshold over `window_size` samples, newest first.

    Only measurements with a gradient length comparable to the latest one are compared.
    """
    if not points:
        return None
    latest_gradient_length = points[0].gradient_length

    for i in range(window_size, len(points)):
        younger, older = points[i - window_size], points[i]

        if not is_within_tolerance(
            older.gradient_length, latest_gradient_length
        ) or not is_within_tolerance(younger.gradient_length, latest_gradient_length):
            continue

        if max(younger.pressure, older.pressure) > MAX_PRESSURE_TO_CONSIDER:
            continue

        if younger.pressure - older.pressure > threshold:
            return PressureChange(younger, older)

    return None


def detect_high(
    points: list[PressureDataPoint], threshold: float
) -> list[PressureDataPoint]:
    """Return the measurements at or above the threshold."""
    return [
        point
        for point in points
        if threshold <= point.pressure <= MAX_PRESSURE_TO_CONSIDER
    ]


def _get_instrument_ids(status_objects: list[KrakenStatus]) -> list[str]:
    return [
        status.id
        for status in status_objects
        if status.entity_type == KrakenStatusEntities.INSTRUMENT
    ]


class PumpPressureIncreaseAlert(BaseAlert):
    """Check for pump pressure increases over a short period."""

    title = "Pump pressure increase detected"
    destinations = frozenset({Destinations.BUSINESS})
    # once per detected change
    cooldown_minutes = None

    def _get_issues(
        self, status_objects: list[KrakenStatus], now: datetime
    ) -> list[Issue[str]]:
        """Check for pump pressure increases per instrument."""
        instrument_ids = _get_instrument_ids(status_objects)
        if not instrument_ids:
            return []

        issues = []
        for instrument_id, points in load_pressure_data(instrument_ids, now).items():
            change = detect_increase(
                points, PUMP_PRESSURE_WINDOW_SIZE, PUMP_PRESSURE_THRESHOLD_BAR
            )
            if change is None:
                continue
            issues.append(
                Issue(
                    instrument_id,
                    f"+{change.increase:.1f} bar (from {change.older.pressure:.1f} bar in "
                    f"`{change.older.raw_file_id}` to {change.younger.pressure:.1f} bar in "
                    f"`{change.younger.raw_file_id}` at {change.younger.created_at:%Y-%m-%d %H:%M:%S})",
                    relevant_file=change.younger.raw_file_id,
                    instrument_id=instrument_id,
                    key=f"{instrument_id}:{change.younger.raw_file_id}:{change.older.raw_file_id}",
                )
            )
        return issues


class PumpPressureHighAlert(BaseAlert):
    """Check for pump pressures above an absolute threshold."""

    title = "High pump pressure detected"
    destinations = frozenset({Destinations.BUSINESS})
    # once per file
    cooldown_minutes = None

    def _get_issues(
        self, status_objects: list[KrakenStatus], now: datetime
    ) -> list[Issue[str]]:
        """Check for high pump pressures per instrument."""
        instrument_ids = _get_instrument_ids(status_objects)
        if not instrument_ids:
            return []

        issues = []
        for instrument_id, points in load_pressure_data(instrument_ids, now).items():
            for point in detect_high(points, PUMP_PRESSURE_ABSOLUTE_THRESHOLD_BAR):
                issues.append(  # noqa: PERF401
                    Issue(
                        instrument_id,
                        f"{point.pressure:.1f} bar (≥{PUMP_PRESSURE_ABSOLUTE_THRESHOLD_BAR} bar) "
                        f"in `{point.raw_file_id}` at {point.created_at:%Y-%m-%d %H:%M:%S}",
                        relevant_file=point.raw_file_id,
                        instrument_id=instrument_id,
                        key=f"{instrument_id}:{point.raw_file_id}",
                    )
                )
        return issues
