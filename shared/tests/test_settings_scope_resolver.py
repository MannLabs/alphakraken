"""Unit tests for scope resolution."""

from unittest.mock import MagicMock, patch

import pytest

from shared.keys import KNOWN_VENDOR_NAMES
from shared.settings_scope_resolver import (
    _scopes_match,
    fully_excluded_scopes,
    known_scopes,
    resolve_scoped_settings,
)


def _make_ps(  # noqa: PLR0913
    scopes: list[str] | None = None,
    software_type: str = "alphadia",
    settings_name: str = "s",
    excluded_scopes: list[str] | None = None,
    raw_file_id_filter: list[str] | None = None,
    raw_file_id_exclude_filter: list[str] | None = None,
) -> MagicMock:
    """Create a mock ProjectSettings."""
    ps = MagicMock()
    ps.scopes = scopes if scopes is not None else ["*"]
    ps.excluded_scopes = excluded_scopes or []
    ps.raw_file_id_filter = raw_file_id_filter or []
    ps.raw_file_id_exclude_filter = raw_file_id_exclude_filter or []
    ps.settings = MagicMock()
    ps.settings.software_type = software_type
    ps.settings.name = settings_name
    return ps


INSTRUMENT_ID = "instrument_1"
INSTRUMENT_TYPE = "bruker"
RAW_FILE_ID = "20240101_plasma_sample.raw"


def _resolve(ps_list: list, raw_file_id: str | None = None) -> list:
    return resolve_scoped_settings(
        ps_list,
        instrument_id=INSTRUMENT_ID,
        instrument_type=INSTRUMENT_TYPE,
        raw_file_id=raw_file_id,
    )


# === _scopes_match ===


def test_scopes_match_default() -> None:
    """Test that '*' matches any instrument."""
    assert _scopes_match(["*"], INSTRUMENT_ID, INSTRUMENT_TYPE)


def test_scopes_match_instrument_id() -> None:
    """Test that an instrument ID matches only that instrument."""
    assert _scopes_match(["instrument_1"], INSTRUMENT_ID, INSTRUMENT_TYPE)
    assert not _scopes_match(["other_instrument"], INSTRUMENT_ID, INSTRUMENT_TYPE)


def test_scopes_match_vendor() -> None:
    """Test that a known vendor name matches on instrument_type only."""
    assert _scopes_match(["bruker"], INSTRUMENT_ID, "bruker")
    assert not _scopes_match(["thermo"], INSTRUMENT_ID, "bruker")


def test_scopes_match_unknown_vendor_matching_instrument_type_returns_false() -> None:
    """Test that an unknown vendor is treated as an instrument ID, not a vendor."""
    assert not _scopes_match(["unknown_vendor"], INSTRUMENT_ID, "unknown_vendor")


def test_scopes_match_any_of_list() -> None:
    """Test that one matching entry suffices."""
    assert _scopes_match(["thermo", "instrument_1"], INSTRUMENT_ID, INSTRUMENT_TYPE)


def test_scopes_match_empty_list_matches_nothing() -> None:
    """Test that an empty list matches nothing."""
    assert not _scopes_match([], INSTRUMENT_ID, INSTRUMENT_TYPE)


# === resolve_scoped_settings: scopes ===


def test_resolve_empty_input_returns_empty() -> None:
    """Test that empty input returns empty list."""
    assert _resolve([]) == []


def test_resolve_no_matching_scopes_returns_empty() -> None:
    """Test that non-matching scopes return empty list."""
    assert _resolve([_make_ps(["other_instrument"])]) == []


def test_resolve_default_scope_matches_any_instrument() -> None:
    """Test that default scope matches any instrument."""
    ps = _make_ps(["*"])
    assert _resolve([ps]) == [ps.settings]


def test_resolve_all_matching_assignments_apply() -> None:
    """Test that default, vendor and instrument scopes all apply, no override."""
    ps_default = _make_ps(["*"], settings_name="default")
    ps_vendor = _make_ps(["bruker"], settings_name="vendor")
    ps_instrument = _make_ps(["instrument_1"], settings_name="instrument")

    result = _resolve([ps_default, ps_vendor, ps_instrument])

    assert result == [ps_default.settings, ps_vendor.settings, ps_instrument.settings]


def test_resolve_same_software_type_twice_keeps_both() -> None:
    """Test that two assignments of the same software_type both apply."""
    ps1 = _make_ps(["*"], "custom", "custom_1")
    ps2 = _make_ps(["*"], "custom", "custom_2")

    assert _resolve([ps1, ps2]) == [ps1.settings, ps2.settings]


def test_resolve_multiple_scopes_match_any() -> None:
    """Test that an assignment with several scopes applies when one matches."""
    ps = _make_ps(["thermo", "sciex", "bruker"])
    assert _resolve([ps]) == [ps.settings]


def test_resolve_same_settings_matched_twice_returned_once() -> None:
    """Test that the same Settings document via two assignments is returned once."""
    ps1 = _make_ps(["*"])
    ps2 = _make_ps(["bruker"])
    ps2.settings = ps1.settings

    assert _resolve([ps1, ps2]) == [ps1.settings]


# === excluded_scopes ===


def test_resolve_excluded_instrument_skipped() -> None:
    """Test that an excluded instrument ID beats a matching scope."""
    ps = _make_ps(["*"], excluded_scopes=["instrument_1"])
    assert _resolve([ps]) == []


def test_resolve_excluded_vendor_skipped() -> None:
    """Test that an excluded vendor beats a matching scope."""
    ps = _make_ps(["*"], excluded_scopes=["bruker"])
    assert _resolve([ps]) == []


def test_resolve_non_excluded_instrument_still_matches() -> None:
    """Test that a non-matching exclusion has no effect."""
    ps = _make_ps(["*"], excluded_scopes=["other_instrument", "thermo"])
    assert _resolve([ps]) == [ps.settings]


def test_resolve_exclusion_does_not_affect_other_assignments() -> None:
    """Test that exclusion is per assignment."""
    ps_default = _make_ps(["*"], settings_name="default")
    ps_vendor = _make_ps(
        ["bruker"], settings_name="vendor", excluded_scopes=["instrument_1"]
    )

    assert _resolve([ps_default, ps_vendor]) == [ps_default.settings]


# === raw_file_id_filter ===


def test_raw_file_id_filter_matches() -> None:
    """Test that settings apply when raw_file_id contains a filter string."""
    ps = _make_ps(raw_file_id_filter=["plasma"])
    assert _resolve([ps], RAW_FILE_ID) == [ps.settings]


def test_raw_file_id_filter_any_entry_matches() -> None:
    """Test that one matching entry of several suffices."""
    ps = _make_ps(raw_file_id_filter=["serum", "plasma"])
    assert _resolve([ps], RAW_FILE_ID) == [ps.settings]


def test_raw_file_id_filter_no_match() -> None:
    """Test that settings are skipped when raw_file_id contains no filter string."""
    ps = _make_ps(raw_file_id_filter=["serum", "urine"])
    assert _resolve([ps], RAW_FILE_ID) == []


def test_raw_file_id_filter_empty_applies_to_all() -> None:
    """Test that an empty filter applies to all files."""
    ps = _make_ps(raw_file_id_filter=[])
    assert _resolve([ps], RAW_FILE_ID) == [ps.settings]


def test_raw_file_id_filter_is_case_sensitive() -> None:
    """Test that matching is case-sensitive."""
    ps = _make_ps(raw_file_id_filter=["Plasma"])
    assert _resolve([ps], RAW_FILE_ID) == []


# === raw_file_id_exclude_filter ===


def test_raw_file_id_exclude_filter_skips_matching_file() -> None:
    """Test that a matching exclude filter skips the assignment."""
    ps = _make_ps(raw_file_id_exclude_filter=["plasma"])
    assert _resolve([ps], RAW_FILE_ID) == []


def test_raw_file_id_exclude_filter_beats_include_filter() -> None:
    """Test that exclude wins when both filters match."""
    ps = _make_ps(raw_file_id_filter=["plasma"], raw_file_id_exclude_filter=["sample"])
    assert _resolve([ps], RAW_FILE_ID) == []


def test_raw_file_id_exclude_filter_is_case_insensitive() -> None:
    """Test that the exclude filter matches regardless of case."""
    ps = _make_ps(raw_file_id_exclude_filter=["PLASMA"])
    assert _resolve([ps], RAW_FILE_ID) == []


def test_raw_file_id_exclude_filter_no_match_applies() -> None:
    """Test that a non-matching exclude filter has no effect."""
    ps = _make_ps(raw_file_id_exclude_filter=["serum"])
    assert _resolve([ps], RAW_FILE_ID) == [ps.settings]


def test_raw_file_id_filters_ignored_when_raw_file_id_is_none() -> None:
    """Test that both file-name filters are ignored for the webapp preview."""
    ps_included = _make_ps(raw_file_id_filter=["nonexistent"], settings_name="inc")
    ps_excluded = _make_ps(raw_file_id_exclude_filter=["plasma"], settings_name="exc")

    assert _resolve([ps_included, ps_excluded]) == [
        ps_included.settings,
        ps_excluded.settings,
    ]


@patch.dict(
    "shared.settings_scope_resolver.YAMLSETTINGS",
    {"instruments": {"test1": {}, "test2": {}}},
    clear=True,
)
def test_known_scopes() -> None:
    """The default scope comes first, then the vendors, then the instruments of the yaml."""
    assert known_scopes() == ["*", *KNOWN_VENDOR_NAMES, "test1", "test2"]


@pytest.mark.parametrize(
    ("scopes", "excluded_scopes", "expected"),
    [
        (["test1"], ["thermo"], ["test1"]),
        (["thermo"], ["test1", "test2"], ["thermo"]),
        (["*"], ["thermo", "test3"], ["*"]),
        (["thermo", "test3"], ["test1"], []),
        # no instrument of the vendor: nothing to exclude
        (["sciex"], ["test1"], []),
    ],
)
@patch.dict(
    "shared.settings_scope_resolver.YAMLSETTINGS",
    {
        "instruments": {
            "test1": {"type": "thermo"},
            "test2": {"type": "thermo"},
            "test3": {"type": "bruker"},
        }
    },
    clear=True,
)
def test_fully_excluded_scopes(
    scopes: list[str], excluded_scopes: list[str], expected: list[str]
) -> None:
    """A scope is fully excluded if each of its instruments matches an excluded scope."""
    assert fully_excluded_scopes(scopes, excluded_scopes) == expected
