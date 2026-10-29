"""Tests for PEAKS metrics."""

import sqlite3
import struct
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from plugins.metrics.metrics.peaks import (
    METRICS,
    _find_run,
    _fragment_mz,
    _half_height,
    _pb,
    calc_peaks_metrics,
    missed_cleavages,
    strip_mods,
)

RUN = "run01"
N_ROWS = 120
MINUTES_PER_SCAN = 0.1 / 81
SCANS_PER_CYCLE = 81
PROTON, H2O = 1.007276467, 18.010565
MZ_Y1_K = 128.094963 + H2O + PROTON  # y1 of ...K
MZ_B2_PE = 97.052764 + 129.042593 + PROTON  # b2 of PE...


# ------------------------------------------------------------ protobuf encoding
def _varint(v: int) -> bytes:
    out = bytearray()
    while True:
        b, v = v & 0x7F, v >> 7
        if not v:
            out.append(b)
            return bytes(out)
        out.append(b | 0x80)


def _len_field(f: int, payload: bytes) -> bytes:
    return _varint(f << 3 | 2) + _varint(len(payload)) + payload


def _f32_field(f: int, x: float) -> bytes:
    return _varint(f << 3 | 5) + struct.pack("<f", x)


def _f32s(xs: list[float]) -> bytes:
    return np.asarray(xs, "<f4").tobytes()


def _fragment_fast_layout(profile: list[float], top: float) -> bytes:
    """The fixed field-1..5 layout PEAKS writes for a fragment chromatogram."""
    return (
        _len_field(1, b"\x00")
        + _len_field(2, b"\x00")
        + _f32_field(3, 0.0)
        + _len_field(4, _f32s(profile))
        + _f32_field(5, top)
    )


def _fragment_generic_layout(profile: list[float], top: float) -> bytes:
    return _len_field(4, _f32s(profile)) + _f32_field(5, top)


def _feature_proto(rt_min: list[float], fragments: list[bytes]) -> bytes:
    return _len_field(7, _f32s(rt_min)) + b"".join(_len_field(3, f) for f in fragments)


def _details_proto(indices: list[int], errors_th: list[float]) -> bytes:
    return _len_field(13, bytes(indices)) + _len_field(14, _f32s(errors_th))


def _lib_proto(names: list[str]) -> bytes:
    return b"".join(_len_field(6, n.encode()) for n in names)


def _psm_details(psm_id: uuid.UUID) -> bytes:
    return _len_field(20, _len_field(6, psm_id.bytes))


# ------------------------------------------------------------------ fixtures
def _tag(block: int) -> str:
    letters = "AGSV"
    return "".join(letters[(block >> (2 * k)) & 3] for k in range(3))


def _precursor_df() -> pd.DataFrame:
    """30 blocks of 4 precursors with hand-checkable aggregates."""
    rows = []
    for i in range(N_ROWS):
        block, kind = divmod(i, 4)
        tag = _tag(block)
        peptide, z, ptm = {
            0: (f"PEPTIDE{tag}K", 2, None),
            1: (f"PEPTIDE{tag}RK", 3, None),
            2: (f"M(+15.99)PEPTIDE{tag}K", 2, "Oxidation (M)"),
            3: (f"MPEPTIDE{tag}K", 1, None),
        }[kind]
        scan = SCANS_PER_CYCLE * i + 1
        rows.append(
            {
                "Peptide": peptide,
                "z": z,
                "-10LgP": 20 + i % 10,
                "RT": scan * MINUTES_PER_SCAN,
                "Scan": scan,
                "MS2 Scans": f"{scan}:{scan + SCANS_PER_CYCLE}:{scan + 2 * SCANS_PER_CYCLE}",
                "Feature Id": i + 1,
                "Area": 1000 * (i + 1) if i % 2 == 0 else np.nan,
                "MS2 Area": 100 * (i + 1),
                "Accession": f"P{i % 12}",
                "ppm": i % 11 - 5,
                "PTM": ptm,
            }
        )
    return pd.DataFrame(rows)


def _write_exports(output_dir: Path) -> None:
    exports = output_dir / "exports"
    exports.mkdir(parents=True)
    _precursor_df().to_csv(exports / f"{RUN}.dia_db.precursor.csv", index=False)
    pd.DataFrame(
        {
            "Protein Group": [1, 1, 2, 3],
            "Top": [True, False, True, True],
            f"#Precursors {RUN}": [5, 5, 0, 2],
            f"Coverage(%) {RUN}": [30.0, 10.0, 50.0, 20.0],
        }
    ).to_csv(exports / "dia_db.proteins.csv", index=False)
    pd.DataFrame(
        {
            "Protein Group": [1, 1, 1, 3, 2, 2],
            "Peptide": ["pepA", "pepB", "pepA", "pepC", "pepD", "pepE"],
            f"#Precursors {RUN}": [1, 1, 1, 1, 0, 0],
        }
    ).to_csv(exports / "dia_db.protein-peptides.csv", index=False)


def _write_project(output_dir: Path) -> None:
    """A PEAKS project with PSMs for Feature Ids 1 and 2."""
    project = output_dir / "some_project_name"
    project.mkdir()
    psm_ids = {1: uuid.uuid4(), 2: uuid.uuid4()}

    with sqlite3.connect(project / "feature_1_dia.db") as c:
        c.execute("create table feature_dia (feature_id, fraction_id, psm_details)")
        c.executemany(
            "insert into feature_dia values (?, ?, ?)",
            [(fid, "f1", _psm_details(pid)) for fid, pid in psm_ids.items()],
        )

    rt_min = [t / 60 for t in range(5)]
    # feature 1: symmetric peak, FWHM 2 s, split over a fast- and a generic-layout fragment
    feat1 = _feature_proto(
        rt_min,
        [
            _fragment_fast_layout([0, 0.5, 1, 0.5, 0], 100.0),
            _fragment_generic_layout([0, 0.5, 1, 0.5, 0], 50.0),
        ],
    )
    # feature 2: tailing peak, FWHM 5/3 s, asymmetry 1.5
    feat2 = _feature_proto(
        rt_min, [_fragment_fast_layout([0, 0.25, 1, 0.5, 0.25], 10.0)]
    )
    rows = [
        (
            str(psm_ids[1]),
            "PEPTIDEK",
            _details_proto([0, 1, 2], [MZ_Y1_K * 10e-6, MZ_B2_PE * 20e-6, 0.0]),
            _lib_proto(["y1", "b2", "p"]),
            feat1,
            0,
        ),
        (
            str(psm_ids[2]),
            "PEPTIDEK",
            _details_proto([0], [MZ_Y1_K * 30e-6]),
            _lib_proto(["y1"]),
            feat2,
            0,
        ),
        (str(uuid.uuid4()), "PEPTIDEK", b"", b"", b"", 1),  # decoy: ignored
    ]
    with sqlite3.connect(project / "dia_psm_task__f1.db") as c:
        c.execute(
            "create table dia_psm (psm_id, sequence, details_proto, lib_proto, feature_proto, decoy)"
        )
        c.executemany("insert into dia_psm values (?, ?, ?, ?, ?, ?)", rows)


# ---------------------------------------------------------------------- tests
def test_calc_peaks_metrics_exports_only(tmp_path: Path) -> None:
    """Metrics from the exports; project-dependent ones are NaN."""
    _write_exports(tmp_path)

    m = calc_peaks_metrics(tmp_path)

    assert set(m) == set(METRICS)
    assert m["n_protein_groups"] == 2
    assert m["n_protein_groups_2pep"] == 1
    assert m["protein_coverage_median"] == 25.0
    assert m["n_peptides"] == 120
    assert m["n_sequences"] == 90
    assert m["n_precursors"] == 120
    assert m["median_score"] == 24.5
    assert m["rt_median_min"] == pytest.approx(5.95 + MINUTES_PER_SCAN)
    assert m["elution_window_min"] == pytest.approx(0.1 * (117.81 - 1.19))
    assert m["cycle_time_s"] == pytest.approx(6.0)
    assert m["total_ms2_area"] == 726_000
    assert m["total_ms1_area"] == 3_600_000
    assert m["frac_with_ms1"] == 0.5
    assert m["median_ms2_area"] == 6050
    lo, hi = np.quantile(np.log10(100 * np.arange(1, N_ROWS + 1)), [0.01, 0.99])
    assert m["ms2_dynamic_range_log10"] == pytest.approx(hi - lo)
    assert np.isnan(m["ms1_dynamic_range_log10"])  # fewer than 100 values
    assert m["top10_protein_ms2_share"] == pytest.approx(1 - 111_000 / 726_000)
    assert m["ms1_ppm_median"] == 0
    assert m["ms1_ppm_mad"] == 3
    assert m["missed_cleavage_rate"] == 0.25
    assert m["met_oxidation_rate"] == 0.5
    assert m["peptide_length_median"] == 12
    assert m["frac_z1"] == 0.25
    assert m["frac_z2"] == 0.5
    assert m["frac_z3"] == 0.25
    assert m["frac_z4"] == 0
    for k in (
        "fwhm_median_s",
        "asymmetry_geomean",
        "frac_tailing",
        "points_per_fwhm",
        "ms2_ppm_median",
        "ms2_ppm_mad",
    ):
        assert np.isnan(m[k]), k


def test_calc_peaks_metrics_with_project(tmp_path: Path) -> None:
    """Peak shape and MS2 mass error from the project database."""
    _write_exports(tmp_path)
    _write_project(tmp_path)

    m = calc_peaks_metrics(tmp_path)

    assert m["fwhm_median_s"] == pytest.approx((2 + 5 / 3) / 2)
    assert m["asymmetry_geomean"] == pytest.approx(np.sqrt(1.5))
    assert m["frac_tailing"] == 0
    assert m["points_per_fwhm"] == pytest.approx((2 + 5 / 3) / 2 / 6)
    assert m["ms2_ppm_median"] == pytest.approx(20, rel=1e-4)
    assert m["ms2_ppm_mad"] == pytest.approx(10, rel=1e-4)
    assert m["n_precursors"] == 120


def test_find_run_requires_exactly_one_precursor_file(tmp_path: Path) -> None:
    """Zero or several precursor exports are an error."""
    with pytest.raises(FileNotFoundError):
        _find_run(tmp_path)

    _write_exports(tmp_path)
    (tmp_path / "exports" / "other.dia_db.precursor.csv").touch()
    with pytest.raises(FileNotFoundError):
        _find_run(tmp_path)


def test_strip_mods() -> None:
    """Modifications in brackets are removed."""
    s = pd.Series(["DLLVENVPYC(+57.02)DAPTQK", "(+42.01)MK", "PEPK"])
    assert strip_mods(s).tolist() == ["DLLVENVPYCDAPTQK", "MK", "PEPK"]


def test_missed_cleavages() -> None:
    """Internal K/R count, the C-terminal one does not."""
    s = pd.Series(["PEPTIDEK", "PEKPTIDER", "KR", "PEPTIDE"])
    assert missed_cleavages(s).tolist() == [0, 1, 1, 0]


def test_half_height_interpolates_crossings() -> None:
    """Half-height crossings are linearly interpolated."""
    rt = np.array([0.0, 1.0, 2.0, 3.0, 4.0])
    y = np.array([0.0, 2.5, 10.0, 5.0, 2.5])
    fwhm, asym = _half_height(rt, y)
    assert fwhm == pytest.approx(5 / 3)
    assert asym == pytest.approx(1.5)


def test_half_height_truncated_window_uses_edge() -> None:
    """A window ending above half height contributes its edge."""
    rt = np.array([0.0, 1.0, 2.0])
    fwhm, asym = _half_height(rt, np.array([0.5, 2.0, 1.5]))
    assert fwhm == pytest.approx(5 / 3)
    assert asym == pytest.approx(1.5)

    fwhm, asym = _half_height(rt, np.array([2.0, 1.0, 0.0]))  # apex on the edge
    assert fwhm == pytest.approx(1.0)
    assert np.isnan(asym)


def test_fragment_mz() -> None:
    """b/y m/z from cumulative residue masses; other ions are NaN."""
    cum = np.cumsum(
        [
            0.0,
            97.052764,
            129.042593,
            97.052764,
            101.047679,
            113.084064,
            115.026943,
            129.042593,
            128.094963,
        ]
    )
    assert _fragment_mz(cum, "y1") == pytest.approx(MZ_Y1_K)
    assert _fragment_mz(cum, "b2") == pytest.approx(MZ_B2_PE)
    assert _fragment_mz(cum, "y1[2+]") == pytest.approx((MZ_Y1_K + PROTON) / 2)
    assert np.isnan(_fragment_mz(cum, "p"))
    assert np.isnan(_fragment_mz(cum, "y9"))


def test_pb_decodes_all_wire_types() -> None:
    """Varint, fixed32, fixed64 and length-delimited fields decode."""
    msg = _varint(1 << 3 | 0) + _varint(300) + _f32_field(2, 1.5) + _len_field(3, b"ab")
    msg += _varint(4 << 3 | 1) + struct.pack("<d", 2.5)
    d = _pb(msg)
    assert d[1] == [300]
    assert d[2] == [1.5]
    assert d[3] == [b"ab"]
    assert d[4] == [2.5]
