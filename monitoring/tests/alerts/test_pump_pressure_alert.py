"""Unit tests for the pump pressure alerts."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest
import pytz

from monitoring.alerts.pump_pressure_alert import (
    PressureChange,
    PressureDataPoint,
    PumpPressureHighAlert,
    PumpPressureIncreaseAlert,
    _detect_high,
    _detect_increase,
    _group_by_instrument,
    _is_within_tolerance,
    _load_pressure_data,
)
from monitoring.utils import short_digest
from shared.db.raw_files import RawFileFilter
from shared.domain.status import KrakenStatusEntities
from shared.keys import MetricsTypes
from shared.tests.factories import metrics, raw_file

NOW = datetime(2024, 1, 15, 12, 0, tzinfo=pytz.UTC)


def _points(
    pressures: list[float], gradient_length: float = 0.5
) -> list[PressureDataPoint]:
    """Build data points newest first, one per hour."""
    return [
        PressureDataPoint(p, gradient_length, NOW - timedelta(hours=i), f"file{i + 1}")
        for i, p in enumerate(pressures)
    ]


class TestGroupByInstrument:
    """Grouping of the raw file metrics per instrument."""

    def test_groups_metrics_by_instrument(self) -> None:
        """Each raw file with a pressure becomes a data point of its instrument."""
        # given
        recent_files = [
            raw_file(id="file1", instrument_id="instrument1", created_at=NOW),
            raw_file(
                id="file2",
                instrument_id="instrument1",
                created_at=NOW - timedelta(hours=1),
            ),
            raw_file(
                id="file3",
                instrument_id="instrument2",
                created_at=NOW - timedelta(hours=2),
            ),
        ]
        latest_metrics = {
            "file1": {
                MetricsTypes.ALPHADIA: metrics(values={"gradient_length": 0.5}),
                MetricsTypes.MSQC: metrics(
                    type=MetricsTypes.MSQC,
                    values={"evosep_pump_hp_pressure_max": 100.0},
                ),
            },
            "file2": {
                MetricsTypes.MSQC: metrics(
                    type=MetricsTypes.MSQC,
                    values={"evosep_pump_hp_pressure_max": 110.0},
                ),
            },
            "file3": {
                MetricsTypes.ALPHADIA: metrics(values={"gradient_length": 0.6}),
                MetricsTypes.MSQC: metrics(type=MetricsTypes.MSQC, values={}),
            },
        }

        # when
        result = _group_by_instrument(recent_files, latest_metrics)

        # then - missing gradient length is kept, missing pressure is skipped
        assert result == {
            "instrument1": [
                PressureDataPoint(100.0, 0.5, NOW, "file1"),
                PressureDataPoint(110.0, None, NOW - timedelta(hours=1), "file2"),
            ],
        }


@pytest.mark.parametrize(
    ("value", "target", "expected"),
    [
        (0.5, 0.5, True),
        (0.54, 0.5, True),
        (0.6, 0.5, False),
        (None, 0.5, False),
        (0.5, None, False),
    ],
)
def test_is_within_tolerance(
    value: float | None, target: float | None, *, expected: bool
) -> None:
    """Values within 10 % of the target are comparable; None never is."""
    assert _is_within_tolerance(value, target) is expected


class TestDetectIncrease:
    """Detection of a pressure increase over a window of samples."""

    def test_detects_increase_above_threshold(self) -> None:
        """The first pair over the window with an increase above the threshold is returned."""
        # given
        points = _points([125.0, 124.0, 123.0, 122.0, 121.0, 120.0, 100.0])

        # when
        result = _detect_increase(points, window_size=5, threshold=20)

        # then - file2 (124) vs file7 (100) is the first pair above 20 bar
        assert result == PressureChange(points[1], points[6])
        assert result.increase == 24.0

    @pytest.mark.parametrize(
        "pressures",
        [
            [110.0, 109.0, 108.0, 107.0, 106.0, 105.0, 100.0],  # below threshold
            [120.0, 115.0, 110.0, 105.0, 100.0, 100.0, 100.0],  # exactly the threshold
            [80.0, 85.0, 90.0, 95.0, 100.0, 105.0, 110.0],  # decrease
            [125.0, 124.0, 123.0, 122.0, 121.0],  # fewer samples than the window
            [1500.0, 124.0, 123.0, 122.0, 121.0, 120.0, 119.0],  # implausible value
        ],
    )
    def test_detects_nothing(self, pressures: list[float]) -> None:
        """No change is returned without an increase strictly above the threshold."""
        assert _detect_increase(_points(pressures), window_size=5, threshold=20) is None

    def test_compares_only_comparable_gradient_lengths(self) -> None:
        """Measurements with a different gradient length than the latest are skipped."""
        # given - the large increase is between a 2 min and a 16 min gradient
        points = [
            *_points([150.0, 148.0, 146.0, 144.0, 142.0, 140.0], gradient_length=16.0),
            PressureDataPoint(110.0, 2.0, NOW - timedelta(hours=6), "file7"),
        ]

        # when / then
        assert _detect_increase(points, window_size=5, threshold=20) is None


class TestDetectHigh:
    """Detection of measurements above the absolute threshold."""

    def test_returns_measurements_at_or_above_threshold(self) -> None:
        """Measurements at or above the threshold are returned, implausible ones skipped."""
        # given
        points = _points([485.0, 480.0, 475.0, 1500.0])

        # when / then
        assert _detect_high(points, threshold=480) == [points[0], points[1]]


class TestLoadPressureData:
    """Loading of the recent raw files and their metrics."""

    @patch("shared.db.metrics.latest_by_raw_file")
    @patch("shared.db.raw_files.find")
    @patch("monitoring.alerts.pump_pressure_alert.PUMP_PRESSURE_LOOKBACK_DAYS", 7)
    def test_returns_empty_without_raw_files(
        self, mock_find: Mock, mock_latest: Mock
    ) -> None:
        """Without raw files in the lookback window, no metrics are loaded."""
        # given
        mock_find.return_value = []

        # when
        result = _load_pressure_data(["instrument1"], NOW)

        # then
        assert result == {}
        mock_find.assert_called_once_with(
            RawFileFilter(
                instrument_ids=["instrument1"], created_after=NOW - timedelta(days=7)
            )
        )
        mock_latest.assert_not_called()

    @patch("shared.db.metrics.latest_by_raw_file")
    @patch("shared.db.raw_files.find")
    def test_groups_the_metrics_of_the_raw_files(
        self, mock_find: Mock, mock_latest: Mock
    ) -> None:
        """The metrics of the found raw files are loaded by field and grouped per instrument."""
        # given
        mock_find.return_value = [
            raw_file(id="file1", instrument_id="instrument1", created_at=NOW)
        ]
        mock_latest.return_value = {
            "file1": {
                MetricsTypes.ALPHADIA: metrics(values={"gradient_length": 0.5}),
                MetricsTypes.MSQC: metrics(
                    type=MetricsTypes.MSQC,
                    values={"evosep_pump_hp_pressure_max": 100.0},
                ),
            }
        }

        # when
        result = _load_pressure_data(["instrument1"], NOW)

        # then
        assert result == {"instrument1": [PressureDataPoint(100.0, 0.5, NOW, "file1")]}
        mock_latest.assert_called_once_with(
            ["file1"], fields=["gradient_length", "evosep_pump_hp_pressure_max"]
        )


INSTRUMENT = Mock(id="instrument1", entity_type=KrakenStatusEntities.INSTRUMENT)
JOB = Mock(id="file_remover", entity_type=KrakenStatusEntities.JOB)


class TestPumpPressureIncreaseAlert:
    """Issues of the increase alert."""

    @patch("monitoring.alerts.pump_pressure_alert._load_pressure_data")
    def test_no_instruments_loads_nothing(self, mock_load: Mock) -> None:
        """Without instruments in the status objects, no data is loaded."""
        assert PumpPressureIncreaseAlert()._get_issues([JOB], NOW) == []
        mock_load.assert_not_called()

    @patch("monitoring.alerts.pump_pressure_alert.PUMP_PRESSURE_WINDOW_SIZE", 5)
    @patch("monitoring.alerts.pump_pressure_alert.PUMP_PRESSURE_THRESHOLD_BAR", 20)
    @patch("monitoring.alerts.pump_pressure_alert._load_pressure_data")
    def test_one_issue_per_instrument_with_an_increase(self, mock_load: Mock) -> None:
        """An increase yields one issue keyed by the two compared files."""
        # given
        mock_load.return_value = {
            "instrument1": _points([125.0, 124.0, 123.0, 122.0, 121.0, 120.0, 100.0]),
            "instrument2": _points([100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0]),
        }

        # when
        result = PumpPressureIncreaseAlert()._get_issues([INSTRUMENT], NOW)

        # then
        mock_load.assert_called_once_with(["instrument1"], NOW)
        assert len(result) == 1
        issue = result[0]
        assert issue.identifier == "instrument1"
        assert issue.instrument_id == "instrument1"
        assert issue.relevant_file == "file2"
        assert issue.key == f"instrument1:{short_digest('file2:file7')}"
        assert issue.details == (
            "+24.0 bar (from 100.0 bar in `file7` to 124.0 bar in `file2` at 2024-01-15 11:00:00)"
        )


class TestPumpPressureHighAlert:
    """Issues of the high pressure alert."""

    @patch(
        "monitoring.alerts.pump_pressure_alert.PUMP_PRESSURE_ABSOLUTE_THRESHOLD_BAR",
        480,
    )
    @patch("monitoring.alerts.pump_pressure_alert._load_pressure_data")
    def test_one_issue_per_file_above_threshold(self, mock_load: Mock) -> None:
        """Each measurement above the threshold yields one issue keyed by the file."""
        # given
        mock_load.return_value = {"instrument1": _points([485.0, 480.0, 475.0])}

        # when
        result = PumpPressureHighAlert()._get_issues([INSTRUMENT, JOB], NOW)

        # then
        assert [(i.identifier, i.relevant_file, i.key) for i in result] == [
            ("instrument1", "file1", "instrument1:file1"),
            ("instrument1", "file2", "instrument1:file2"),
        ]
        assert (
            result[0].details
            == "485.0 bar (≥480 bar) in `file1` at 2024-01-15 12:00:00"
        )
