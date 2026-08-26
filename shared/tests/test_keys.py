"""Tests for keys."""

from shared.keys import (
    SOFTWARE_TYPE_TO_METRICS_TYPES,
    MetricsTypes,
    SoftwareTypes,
)


def test_software_type_to_metrics_types_covers_all_software_types() -> None:
    """Test that every software type maps to metrics types."""
    assert set(SOFTWARE_TYPE_TO_METRICS_TYPES) == set(SoftwareTypes.get_values())


def test_software_type_to_metrics_types_maps_to_known_metrics_types() -> None:
    """Test that the mapping only yields existing metrics types."""
    mapped_metrics_types = {
        metrics_type
        for metrics_types in SOFTWARE_TYPE_TO_METRICS_TYPES.values()
        for metrics_type in metrics_types
    }

    assert mapped_metrics_types <= set(MetricsTypes.get_values())


def test_software_type_to_metrics_types_has_no_duplicates() -> None:
    """Test that no software type offers the same metrics type twice."""
    for software_type, metrics_types in SOFTWARE_TYPE_TO_METRICS_TYPES.items():
        assert len(metrics_types) == len(set(metrics_types)), software_type


def test_software_type_to_metrics_types_is_not_empty_per_software_type() -> None:
    """Test that every software type has at least one metrics type, as the first is the default."""
    for software_type, metrics_types in SOFTWARE_TYPE_TO_METRICS_TYPES.items():
        assert metrics_types, software_type
