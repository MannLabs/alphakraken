"""Unit tests for scope resolution."""

from unittest.mock import MagicMock

from shared.settings_scope_resolver import (
    SCOPE_LEVEL_DEFAULT,
    SCOPE_LEVEL_INSTRUMENT,
    SCOPE_LEVEL_VENDOR,
    _classify_scope,
    resolve_scoped_settings,
)


def _make_ps(
    scope: str,
    software_type: str,
    settings_name: str = "s",
    excluded: list[str] | None = None,
    raw_file_id_filter: str = "",
) -> MagicMock:
    """Create a mock ProjectSettings with given scope and software_type."""
    ps = MagicMock()
    ps.scope = scope
    ps.excluded = excluded or []
    ps.raw_file_id_filter = raw_file_id_filter
    ps.settings = MagicMock()
    ps.settings.software_type = software_type
    ps.settings.name = settings_name
    return ps


INSTRUMENT_ID = "instrument_1"
INSTRUMENT_TYPE = "bruker"


# === _classify_scope ===


def test_classify_default_scope_returns_level_default() -> None:
    """Test that default scope '*' returns SCOPE_LEVEL_DEFAULT."""
    assert _classify_scope("*", INSTRUMENT_ID, INSTRUMENT_TYPE) == SCOPE_LEVEL_DEFAULT


def test_classify_matching_instrument_id_returns_level_instrument() -> None:
    """Test that matching instrument_id returns SCOPE_LEVEL_INSTRUMENT."""
    assert (
        _classify_scope("instrument_1", "instrument_1", INSTRUMENT_TYPE)
        == SCOPE_LEVEL_INSTRUMENT
    )


def test_classify_matching_vendor_returns_level_vendor() -> None:
    """Test that matching vendor name returns SCOPE_LEVEL_VENDOR."""
    assert _classify_scope("bruker", INSTRUMENT_ID, "bruker") == SCOPE_LEVEL_VENDOR


def test_classify_non_matching_scope_returns_none() -> None:
    """Test that non-matching scope returns None."""
    assert _classify_scope("other_instrument", INSTRUMENT_ID, INSTRUMENT_TYPE) is None


def test_classify_vendor_name_not_matching_instrument_type_returns_none() -> None:
    """Test that a known vendor not matching instrument_type returns None."""
    assert _classify_scope("thermo", INSTRUMENT_ID, "bruker") is None


def test_classify_unknown_vendor_matching_instrument_type_returns_none() -> None:
    """Test that an unknown vendor matching instrument_type returns None."""
    assert _classify_scope("unknown_vendor", INSTRUMENT_ID, "unknown_vendor") is None


# === resolve_scoped_settings ===


def test_resolve_empty_input_returns_empty() -> None:
    """Test that empty input returns empty list."""
    assert (
        resolve_scoped_settings(
            [], instrument_id=INSTRUMENT_ID, instrument_type=INSTRUMENT_TYPE
        )
        == []
    )


def test_resolve_no_matching_scopes_returns_empty() -> None:
    """Test that non-matching scopes return empty list."""
    ps = _make_ps("other_instrument", "alphadia")
    assert (
        resolve_scoped_settings(
            [ps], instrument_id=INSTRUMENT_ID, instrument_type=INSTRUMENT_TYPE
        )
        == []
    )


def test_resolve_default_scope_matches_any_instrument() -> None:
    """Test that default scope matches any instrument."""
    ps = _make_ps("*", "alphadia", "default_settings")
    result = resolve_scoped_settings(
        [ps], instrument_id=INSTRUMENT_ID, instrument_type=INSTRUMENT_TYPE
    )
    assert result == [ps.settings]


def test_resolve_vendor_scope_replaces_default_for_same_software_type() -> None:
    """Test that vendor scope replaces default for the same software_type."""
    ps_default = _make_ps("*", "alphadia", "default_settings")
    ps_vendor = _make_ps("bruker", "alphadia", "bruker_settings")

    result = resolve_scoped_settings(
        [ps_default, ps_vendor],
        instrument_id=INSTRUMENT_ID,
        instrument_type=INSTRUMENT_TYPE,
    )
    assert result == [ps_vendor.settings]


def test_resolve_instrument_scope_replaces_vendor_and_default() -> None:
    """Test that instrument scope replaces both vendor and default."""
    ps_default = _make_ps("*", "alphadia", "default_settings")
    ps_vendor = _make_ps("bruker", "alphadia", "bruker_settings")
    ps_instrument = _make_ps("instrument_1", "alphadia", "instrument_settings")

    result = resolve_scoped_settings(
        [ps_default, ps_vendor, ps_instrument],
        instrument_id=INSTRUMENT_ID,
        instrument_type=INSTRUMENT_TYPE,
    )
    assert result == [ps_instrument.settings]


def test_resolve_multiple_same_level_entries_first_wins() -> None:
    """Test that duplicate entries at the same scope level keep only the first."""
    ps_default_1 = _make_ps("*", "alphadia", "settings_1")
    ps_default_2 = _make_ps("*", "alphadia", "settings_2")

    result = resolve_scoped_settings(
        [ps_default_1, ps_default_2],
        instrument_id=INSTRUMENT_ID,
        instrument_type=INSTRUMENT_TYPE,
    )
    assert result == [ps_default_1.settings]


def test_resolve_mixed_software_types_resolved_independently() -> None:
    """Test that different software_types are resolved independently."""
    ps_alphadia_default = _make_ps("*", "alphadia", "alphadia_default")
    ps_alphadia_vendor = _make_ps("bruker", "alphadia", "alphadia_bruker")
    ps_msqc_default = _make_ps("*", "msqc", "msqc_default")

    result = resolve_scoped_settings(
        [ps_alphadia_default, ps_alphadia_vendor, ps_msqc_default],
        instrument_id=INSTRUMENT_ID,
        instrument_type=INSTRUMENT_TYPE,
    )
    assert set(result) == {ps_alphadia_vendor.settings, ps_msqc_default.settings}


# === exclusions ===


def test_resolve_excluded_instrument_skipped_on_default_scope() -> None:
    """Test that an excluded instrument is skipped even when default scope matches."""
    ps = _make_ps("*", "alphadia", "settings1", excluded=["instrument_1"])
    result = resolve_scoped_settings(
        [ps], instrument_id=INSTRUMENT_ID, instrument_type=INSTRUMENT_TYPE
    )
    assert result == []


def test_resolve_non_excluded_instrument_still_matches() -> None:
    """Test that a non-excluded instrument still matches normally."""
    ps = _make_ps("*", "alphadia", "settings1", excluded=["other_instrument"])
    result = resolve_scoped_settings(
        [ps], instrument_id=INSTRUMENT_ID, instrument_type=INSTRUMENT_TYPE
    )
    assert result == [ps.settings]


def test_resolve_excluded_instrument_skipped_on_vendor_scope() -> None:
    """Test that an excluded instrument is skipped even with vendor scope match."""
    ps = _make_ps("bruker", "alphadia", "settings1", excluded=["instrument_1"])
    result = resolve_scoped_settings(
        [ps], instrument_id=INSTRUMENT_ID, instrument_type=INSTRUMENT_TYPE
    )
    assert result == []


def test_resolve_excluded_does_not_fall_through_to_lower_level() -> None:
    """Test that excluding at a higher level does not fall through to a lower-level entry."""
    ps_default = _make_ps("*", "alphadia", "default_settings")
    ps_vendor = _make_ps(
        "bruker", "alphadia", "bruker_settings", excluded=["instrument_1"]
    )

    result = resolve_scoped_settings(
        [ps_default, ps_vendor],
        instrument_id=INSTRUMENT_ID,
        instrument_type=INSTRUMENT_TYPE,
    )
    assert result == [ps_default.settings]


def test_resolve_empty_excluded_list_behaves_like_no_exclusion() -> None:
    """Test that an empty excluded list has no effect."""
    ps = _make_ps("*", "alphadia", "settings1", excluded=[])
    result = resolve_scoped_settings(
        [ps], instrument_id=INSTRUMENT_ID, instrument_type=INSTRUMENT_TYPE
    )
    assert result == [ps.settings]


# === raw_file_id_filter ===

RAW_FILE_ID = "20240101_plasma_sample.raw"


def test_raw_file_id_filter_matches() -> None:
    """Test that settings apply when raw_file_id contains the filter string."""
    ps = _make_ps("*", "alphadia", raw_file_id_filter="plasma")
    result = resolve_scoped_settings(
        [ps],
        instrument_id=INSTRUMENT_ID,
        instrument_type=INSTRUMENT_TYPE,
        raw_file_id=RAW_FILE_ID,
    )
    assert result == [ps.settings]


def test_raw_file_id_filter_no_match() -> None:
    """Test that settings are skipped when raw_file_id doesn't contain the filter string."""
    ps = _make_ps("*", "alphadia", raw_file_id_filter="serum")
    result = resolve_scoped_settings(
        [ps],
        instrument_id=INSTRUMENT_ID,
        instrument_type=INSTRUMENT_TYPE,
        raw_file_id=RAW_FILE_ID,
    )
    assert result == []


def test_raw_file_id_filter_empty_applies_to_all() -> None:
    """Test that an empty filter applies to all files."""
    ps = _make_ps("*", "alphadia", raw_file_id_filter="")
    result = resolve_scoped_settings(
        [ps],
        instrument_id=INSTRUMENT_ID,
        instrument_type=INSTRUMENT_TYPE,
        raw_file_id=RAW_FILE_ID,
    )
    assert result == [ps.settings]


def test_raw_file_id_filter_none_raw_file_id_skips_filter() -> None:
    """Test that when raw_file_id is None, the filter is not applied."""
    ps = _make_ps("*", "alphadia", raw_file_id_filter="nonexistent")
    result = resolve_scoped_settings(
        [ps], instrument_id=INSTRUMENT_ID, instrument_type=INSTRUMENT_TYPE
    )
    assert result == [ps.settings]


def test_raw_file_id_filter_is_case_sensitive() -> None:
    """Test that matching is case-sensitive."""
    ps = _make_ps("*", "alphadia", raw_file_id_filter="Plasma")
    result = resolve_scoped_settings(
        [ps],
        instrument_id=INSTRUMENT_ID,
        instrument_type=INSTRUMENT_TYPE,
        raw_file_id=RAW_FILE_ID,
    )
    assert result == []


def test_raw_file_id_filter_fallback_to_lower_scope() -> None:
    """Test that when a higher-scope entry is filtered out, lower-scope entries win."""
    ps_default = _make_ps("*", "alphadia", "default_settings")
    ps_vendor = _make_ps(
        "bruker", "alphadia", "bruker_settings", raw_file_id_filter="serum"
    )
    result = resolve_scoped_settings(
        [ps_default, ps_vendor],
        instrument_id=INSTRUMENT_ID,
        instrument_type=INSTRUMENT_TYPE,
        raw_file_id=RAW_FILE_ID,
    )
    assert result == [ps_default.settings]


def test_raw_file_id_filter_match_beats_unfiltered_at_same_scope() -> None:
    """Test that a matching filter entry wins over an unfiltered entry at the same scope level."""
    ps_unfiltered = _make_ps("*", "alphadia", "unfiltered_settings")
    ps_filtered = _make_ps(
        "*", "alphadia", "filtered_settings", raw_file_id_filter="plasma"
    )
    result = resolve_scoped_settings(
        [ps_unfiltered, ps_filtered],
        instrument_id=INSTRUMENT_ID,
        instrument_type=INSTRUMENT_TYPE,
        raw_file_id=RAW_FILE_ID,
    )
    assert result == [ps_filtered.settings]


def test_raw_file_id_filter_no_priority_when_raw_file_id_is_none() -> None:
    """Test that filter does not give priority when raw_file_id is None (webapp preview)."""
    ps_unfiltered = _make_ps("*", "alphadia", "unfiltered_settings")
    ps_filtered = _make_ps(
        "*", "alphadia", "filtered_settings", raw_file_id_filter="plasma"
    )
    result = resolve_scoped_settings(
        [ps_unfiltered, ps_filtered],
        instrument_id=INSTRUMENT_ID,
        instrument_type=INSTRUMENT_TYPE,
    )
    assert result == [ps_unfiltered.settings]
