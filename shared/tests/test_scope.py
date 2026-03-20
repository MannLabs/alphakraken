"""Unit tests for scope resolution."""

from unittest.mock import MagicMock

from shared.scope import (
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
) -> MagicMock:
    """Create a mock ProjectSettings with given scope and software_type."""
    ps = MagicMock()
    ps.scope = scope
    ps.excluded = excluded or []
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
    assert resolve_scoped_settings([], INSTRUMENT_ID, INSTRUMENT_TYPE) == []


def test_resolve_no_matching_scopes_returns_empty() -> None:
    """Test that non-matching scopes return empty list."""
    ps = _make_ps("other_instrument", "alphadia")
    assert resolve_scoped_settings([ps], INSTRUMENT_ID, INSTRUMENT_TYPE) == []


def test_resolve_default_scope_matches_any_instrument() -> None:
    """Test that default scope matches any instrument."""
    ps = _make_ps("*", "alphadia", "default_settings")
    result = resolve_scoped_settings([ps], INSTRUMENT_ID, INSTRUMENT_TYPE)
    assert result == [ps.settings]


def test_resolve_vendor_scope_replaces_default_for_same_software_type() -> None:
    """Test that vendor scope replaces default for the same software_type."""
    ps_default = _make_ps("*", "alphadia", "default_settings")
    ps_vendor = _make_ps("bruker", "alphadia", "bruker_settings")

    result = resolve_scoped_settings(
        [ps_default, ps_vendor], INSTRUMENT_ID, INSTRUMENT_TYPE
    )
    assert result == [ps_vendor.settings]


def test_resolve_instrument_scope_replaces_vendor_and_default() -> None:
    """Test that instrument scope replaces both vendor and default."""
    ps_default = _make_ps("*", "alphadia", "default_settings")
    ps_vendor = _make_ps("bruker", "alphadia", "bruker_settings")
    ps_instrument = _make_ps("instrument_1", "alphadia", "instrument_settings")

    result = resolve_scoped_settings(
        [ps_default, ps_vendor, ps_instrument], INSTRUMENT_ID, INSTRUMENT_TYPE
    )
    assert result == [ps_instrument.settings]


def test_resolve_multiple_same_level_entries_all_returned() -> None:
    """Test that multiple entries at the same scope level are all returned."""
    ps_default_1 = _make_ps("*", "alphadia", "settings_1")
    ps_default_2 = _make_ps("*", "alphadia", "settings_2")

    result = resolve_scoped_settings(
        [ps_default_1, ps_default_2], INSTRUMENT_ID, INSTRUMENT_TYPE
    )
    assert result == [ps_default_1.settings, ps_default_2.settings]


def test_resolve_mixed_software_types_resolved_independently() -> None:
    """Test that different software_types are resolved independently."""
    ps_alphadia_default = _make_ps("*", "alphadia", "alphadia_default")
    ps_alphadia_vendor = _make_ps("bruker", "alphadia", "alphadia_bruker")
    ps_msqc_default = _make_ps("*", "msqc", "msqc_default")

    result = resolve_scoped_settings(
        [ps_alphadia_default, ps_alphadia_vendor, ps_msqc_default],
        INSTRUMENT_ID,
        INSTRUMENT_TYPE,
    )
    assert set(result) == {ps_alphadia_vendor.settings, ps_msqc_default.settings}


# === exclusions ===


def test_resolve_excluded_instrument_skipped_on_default_scope() -> None:
    """Test that an excluded instrument is skipped even when default scope matches."""
    ps = _make_ps("*", "alphadia", "settings1", excluded=["instrument_1"])
    result = resolve_scoped_settings([ps], INSTRUMENT_ID, INSTRUMENT_TYPE)
    assert result == []


def test_resolve_non_excluded_instrument_still_matches() -> None:
    """Test that a non-excluded instrument still matches normally."""
    ps = _make_ps("*", "alphadia", "settings1", excluded=["other_instrument"])
    result = resolve_scoped_settings([ps], INSTRUMENT_ID, INSTRUMENT_TYPE)
    assert result == [ps.settings]


def test_resolve_excluded_instrument_skipped_on_vendor_scope() -> None:
    """Test that an excluded instrument is skipped even with vendor scope match."""
    ps = _make_ps("bruker", "alphadia", "settings1", excluded=["instrument_1"])
    result = resolve_scoped_settings([ps], INSTRUMENT_ID, INSTRUMENT_TYPE)
    assert result == []


def test_resolve_excluded_does_not_fall_through_to_lower_level() -> None:
    """Test that excluding at a higher level does not fall through to a lower-level entry."""
    ps_default = _make_ps("*", "alphadia", "default_settings")
    ps_vendor = _make_ps(
        "bruker", "alphadia", "bruker_settings", excluded=["instrument_1"]
    )

    result = resolve_scoped_settings(
        [ps_default, ps_vendor], INSTRUMENT_ID, INSTRUMENT_TYPE
    )
    assert result == [ps_default.settings]


def test_resolve_empty_excluded_list_behaves_like_no_exclusion() -> None:
    """Test that an empty excluded list has no effect."""
    ps = _make_ps("*", "alphadia", "settings1", excluded=[])
    result = resolve_scoped_settings([ps], INSTRUMENT_ID, INSTRUMENT_TYPE)
    assert result == [ps.settings]
