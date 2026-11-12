"""Pump pressure alerts: significant increases over a short period, and pressures above an absolute threshold."""

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any

from utils import short_digest

from shared.db import metrics as metrics_store
from shared.db import raw_files as raw_file_store
from shared.domain.metrics import Metrics
from shared.domain.raw_files import RawFile
from shared.domain.status import KrakenStatus, KrakenStatusEntities
from shared.keys import MetricsTypes

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

GRADIENT_LENGTH_KEY = "gradient_length"
PRESSURE_KEY = "evosep_pump_hp_pressure_max"


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


@lru_cache(maxsize=1)
def _pressure_data(
    instrument_ids: tuple[str, ...], now: datetime
) -> dict[str, list[PressureDataPoint]]:
    """Return the pressure measurements, loaded once per check.

    Both pump pressure alerts run in the same loop iteration with the same `now`, so the
    second one gets the data the first one loaded.
    """
    return _load_pressure_data(list(instrument_ids), now)


def _load_pressure_data(
    instrument_ids: list[str], now: datetime
) -> dict[str, list[PressureDataPoint]]:
    """Load the pressure measurements of the recent raw files per instrument, newest first."""
    cutoff = now - timedelta(days=PUMP_PRESSURE_LOOKBACK_DAYS)
    recent_files = raw_file_store.find(
        raw_file_store.RawFileFilter(
            instrument_ids=instrument_ids, created_after=cutoff
        )
    )
    if not recent_files:
        logging.debug("No raw files found in lookback window")
        return {}

    latest_metrics = metrics_store.latest_by_raw_file(
        [raw_file.id for raw_file in recent_files],
        fields=[GRADIENT_LENGTH_KEY, PRESSURE_KEY],
    )
    return _group_by_instrument(recent_files, latest_metrics)


def _group_by_instrument(
    recent_files: list[RawFile], latest_metrics: dict[str, dict[str, Metrics]]
) -> dict[str, list[PressureDataPoint]]:
    """Group the measurements by instrument in the order of the raw files, skipping files without pressure."""
    pressure_data = defaultdict(list)
    for raw_file in recent_files:
        by_type = latest_metrics.get(raw_file.id, {})
        gradient_length = _value(by_type, MetricsTypes.ALPHADIA, GRADIENT_LENGTH_KEY)
        pressure = _value(by_type, MetricsTypes.MSQC, PRESSURE_KEY)

        if pressure is not None:
            pressure_data[raw_file.instrument_id].append(
                PressureDataPoint(
                    pressure, gradient_length, raw_file.created_at, raw_file.id
                )
            )

    return pressure_data


def _value(by_type: dict[str, Metrics], metrics_type: str, key: str) -> Any:
    """Return a metric value, None if the type or the key is missing."""
    type_metrics = by_type.get(metrics_type)
    return None if type_metrics is None else type_metrics.values.get(key)


def _is_within_tolerance(
    value: float | None,
    target: float | None,
    tolerance: float = PUMP_PRESSURE_GRADIENT_TOLERANCE,
) -> bool:
    """Check if value is within relative tolerance of target, False if either is None."""
    if value is None or target is None:
        return False
    return (1 - tolerance) < (value / target) < (1 + tolerance)


def _detect_increase(
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

        if not _is_within_tolerance(
            older.gradient_length, latest_gradient_length
        ) or not _is_within_tolerance(younger.gradient_length, latest_gradient_length):
            continue

        if max(younger.pressure, older.pressure) > MAX_PRESSURE_TO_CONSIDER:
            continue

        if younger.pressure - older.pressure > threshold:
            return PressureChange(younger, older)

    return None


def _detect_high(
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
    destinations = frozenset({Destinations.BUSINESS, Destinations.USER})
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
        for instrument_id, points in _pressure_data(tuple(instrument_ids), now).items():
            change = _detect_increase(
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
                    # digest: two file ids would exceed the key length
                    key=f"{instrument_id}:{short_digest(f'{change.younger.raw_file_id}:{change.older.raw_file_id}')}",
                )
            )
        return issues


class PumpPressureHighAlert(BaseAlert):
    """Check for pump pressures above an absolute threshold."""

    title = "High pump pressure detected"
    destinations = frozenset({Destinations.BUSINESS, Destinations.USER})
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
        for instrument_id, points in _pressure_data(tuple(instrument_ids), now).items():
            for point in _detect_high(points, PUMP_PRESSURE_ABSOLUTE_THRESHOLD_BAR):
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
