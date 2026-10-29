"""Metrics for PEAKS 13.x CLI (DIA db search) output.

Two sources inside the output directory:

- `exports/`: `<run>.<search type>.precursor.csv`, `<search type>.proteins.csv` and
  `<search type>.protein-peptides.csv` - identifications, RT, areas, MS1 mass error, chemistry.
- the PEAKS project folder next to it (SQLite): the per-fragment chromatograms and mass errors
  PEAKS stores for every PSM but does not export. They give FWHM, half-height asymmetry and
  MS2 mass error; NaN if the folder is missing.

Metric definitions are in `METRICS`.
"""

import logging
import re
import sqlite3
import struct
import uuid
from collections import defaultdict
from contextlib import closing
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

PRECURSOR_SUFFIX = ".precursor.csv"  # exports/<run>.<search type>.precursor.csv
PROTEINS_SUFFIX = ".proteins.csv"
PROTEIN_PEPTIDES_SUFFIX = ".protein-peptides.csv"
FEATURE_DB_GLOB = "feature_*_dia.db"
PSM_DB_GLOB = "dia_psm_*.db"

TAILING_CUTOFF = 1.5  # half-height asymmetry above this counts as a tailing peak
MAX_CHARGE_BIN = 4  # charges >= this share one bin
MIN_FEATURES_FOR_DYNAMIC_RANGE = 100
CYCLE_TIME_SAMPLE_SIZE = 5000
P1, P99 = 0.01, 0.99
SECONDS_PER_MINUTE = 60
PPM_FACTOR = 1e6
TOP_N_PROTEINS = 10


class PrecursorColumns:
    """Column names of the precursor export."""

    PEPTIDE = "Peptide"
    CHARGE = "z"
    SCORE = "-10LgP"
    RT = "RT"
    SCAN = "Scan"
    MS2_SCANS = "MS2 Scans"
    FEATURE_ID = "Feature Id"
    AREA = "Area"
    MS2_AREA = "MS2 Area"
    ACCESSION = "Accession"
    PPM = "ppm"
    PTM = "PTM"
    SEQUENCE = "sequence"  # derived: peptide without modifications


class ProteinColumns:
    """Column names of the protein and protein-peptide exports ({run} is the run name)."""

    PROTEIN_GROUP = "Protein Group"
    TOP = "Top"
    PEPTIDE = "Peptide"
    N_PRECURSORS = "#Precursors {run}"
    COVERAGE = "Coverage(%) {run}"


# name -> (unit, definition)
METRICS = {
    # --- identifications -------------------------------------------------
    "n_protein_groups": (
        "",
        "protein groups with >=1 precursor in this run (1% PG FDR, PEAKS report filter)",
    ),
    "n_protein_groups_2pep": (
        "",
        "protein groups with >=2 distinct peptides in this run",
    ),
    "n_peptides": ("", "distinct modified peptide sequences"),
    "n_sequences": ("", "distinct stripped (unmodified) sequences"),
    "n_precursors": ("", "distinct modified peptide + charge"),
    "median_score": ("-10lgP", "median precursor -10lgP"),
    # --- chromatography --------------------------------------------------
    "rt_median_min": ("min", "median apex RT of identified precursors"),
    "elution_window_min": (
        "min",
        "P99 - P1 of precursor apex RT: the part of the gradient carrying peptides",
    ),
    "fwhm_median_s": (
        "s",
        "median FWHM over all peaks: width at half the apex height of the summed "
        "fragment chromatogram (PEAKS internal XICs), crossings linearly interpolated; "
        "where PEAKS' stored window ends above half height the window edge is used",
    ),
    "asymmetry_geomean": (
        "",
        "geometric mean of the half-height asymmetry (trailing / leading half-width) "
        "of the same chromatograms; >1 = tailing",
    ),
    "frac_tailing": (
        "",
        f"fraction of those peaks with half-height asymmetry > {TAILING_CUTOFF}",
    ),
    "cycle_time_s": (
        "s",
        "DIA cycle time, from the scan-number step between a precursor's MS2 scans "
        "and the RT-per-scan slope",
    ),
    "points_per_fwhm": (
        "",
        "fwhm_median_s / cycle_time_s: samples across the peak at half height "
        "(Nyquist limit 2)",
    ),
    # --- signal ----------------------------------------------------------
    "total_ms2_area": ("", "sum of precursor MS2 (fragment) areas"),
    "total_ms1_area": (
        "",
        "sum of precursor MS1 areas, over precursors that HAVE one (see frac_with_ms1)",
    ),
    "frac_with_ms1": (
        "",
        "fraction of precursors with an MS1 area - itself an MS1 sensitivity readout",
    ),
    "median_ms2_area": ("", "median precursor MS2 area"),
    "ms2_dynamic_range_log10": ("log10", "P1-P99 span of precursor MS2 area"),
    "ms1_dynamic_range_log10": (
        "log10",
        "P1-P99 span of precursor MS1 area (precursors with one)",
    ),
    "top10_protein_ms2_share": (
        "",
        "share of total MS2 area carried by the 10 most intense proteins "
        "(composition: albumin/keratin/histone carry-over, overloaded proteins)",
    ),
    "protein_coverage_median": (
        "%",
        "median sequence coverage of identified protein groups",
    ),
    # --- mass accuracy ---------------------------------------------------
    "ms1_ppm_median": (
        "ppm",
        "median precursor mass error = accuracy (precursors with an MS1 feature; "
        "all reported values, including PEAKS' exact 0.0)",
    ),
    "ms1_ppm_mad": (
        "ppm",
        "median absolute deviation of the precursor mass error = precision "
        "(unscaled; x1.4826 for a Gaussian-sigma equivalent)",
    ),
    "ms2_ppm_median": (
        "ppm",
        "median fragment mass error over the library fragments of the precursors "
        "= accuracy",
    ),
    "ms2_ppm_mad": (
        "ppm",
        "median absolute deviation of the fragment mass error = precision (unscaled)",
    ),
    # --- sample prep / chemistry -----------------------------------------
    "missed_cleavage_rate": (
        "",
        "fraction of peptides with >=1 internal K/R (K/R before P counts as a "
        "cleavage site, so an internal KP/RP is a missed cleavage)",
    ),
    "met_oxidation_rate": (
        "",
        "fraction of M-containing peptides carrying Oxidation (M)",
    ),
    "peptide_length_median": ("aa", "median stripped peptide length"),
    "frac_z1": (
        "",
        "fraction of precursors at charge 1 (high = contamination/polymer)",
    ),
    "frac_z2": ("", "fraction at charge 2"),
    "frac_z3": ("", "fraction at charge 3"),
    "frac_z4": ("", f"fraction at charge >={MAX_CHARGE_BIN}"),
}


# ---------------------------------------------------------------- discovery
_PRECURSOR_FILE = re.compile(
    r"^(?P<run>.+)\.(?P<search_type>[^.]+)" + re.escape(PRECURSOR_SUFFIX) + "$"
)


@dataclass(frozen=True)
class _Run:
    """Files of one PEAKS run."""

    run: str
    precursor_file: Path
    protein_file: Path
    protein_peptide_file: Path
    project_dirs: list[Path]


def _project_dirs(search_dir: Path) -> list[Path]:
    """PEAKS project folders (SQLite databases) next to `exports/`.

    The folder is named after the PEAKS project, which for a CLI run is the run
    name but for a multi-run project is not - so find it by content.
    """
    return [
        d
        for d in sorted(search_dir.iterdir())
        if d.is_dir() and any(d.glob(FEATURE_DB_GLOB))
    ]


def _find_run(output_directory: Path) -> _Run:
    """The single `<run>.<search type>.precursor.csv` below `output_directory` and its companions."""
    matches = [
        (p, m)
        for p in sorted(output_directory.rglob(f"*{PRECURSOR_SUFFIX}"))
        if (m := _PRECURSOR_FILE.match(p.name)) is not None
    ]
    if len(matches) != 1:
        raise FileNotFoundError(
            f"expected one *.<search type>{PRECURSOR_SUFFIX} below {output_directory}, found {len(matches)}"
        )
    p, m = matches[0]
    exports, kind = p.parent, m["search_type"]
    return _Run(
        run=m["run"],
        precursor_file=p,
        protein_file=exports / f"{kind}{PROTEINS_SUFFIX}",
        protein_peptide_file=exports / f"{kind}{PROTEIN_PEPTIDES_SUFFIX}",
        project_dirs=_project_dirs(exports.parent),
    )


# ------------------------------------------------------------------ helpers
_MOD = re.compile(r"\([^)]*\)")


def strip_mods(peptide: pd.Series) -> pd.Series:
    """'DLLVENVPYC(+57.02)DAPTQK' -> 'DLLVENVPYCDAPTQK'."""
    return peptide.str.replace(_MOD, "", regex=True)


def missed_cleavages(seq: pd.Series) -> pd.Series:
    """Internal K/R on stripped sequences; the C-terminal one is the cut.

    No proline rule: K/R before P is a cleavage site, so KP/RP counts as missed.
    """
    return seq.str[:-1].str.count("[KR]")


def _mad(x: np.ndarray | pd.Series) -> float:
    x = np.asarray(x, float)
    x = x[np.isfinite(x)]
    if x.size < 2:  # noqa: PLR2004
        return np.nan
    return float(np.median(np.abs(x - np.median(x))))


def _dynamic_range(area: pd.Series) -> float:
    """log10 P1-P99 span; the full min-max is set by single outlier features."""
    a = area[area > 0]
    if len(a) < MIN_FEATURES_FOR_DYNAMIC_RANGE:
        return np.nan
    lo, hi = np.log10(a).quantile([P1, P99])
    return float(hi - lo)


def measured_ppm(prec: pd.DataFrame) -> pd.Series:
    """Precursor mass error of every precursor that has one (MS1 feature).

    Note: 11% of values are exactly 0.0, against ~1% expected from the
    neighbouring bins - PEAKS appears to write 0.0 for some unmeasured errors.
    They are kept, so the median sits near 0 and the MAD is slightly narrowed.
    """
    return prec[PrecursorColumns.PPM].dropna()


def _scan_lists(col: pd.Series) -> pd.Series:
    return col.astype(str).str.split(":").map(lambda v: np.asarray(v, dtype=np.int64))


def cycle_time_s(prec: pd.DataFrame) -> float:
    """Cycle time from the export alone.

    The scan numbers of one precursor's MS2 scans step by exactly one cycle (80
    windows + 1 MS1 = 81 here), and apex scan number vs apex RT is linear, so
    cycle = step x minutes-per-scan.
    """
    step = (
        _scan_lists(prec[PrecursorColumns.MS2_SCANS].head(CYCLE_TIME_SAMPLE_SIZE))
        .map(lambda v: np.median(np.diff(v)) if v.size > 1 else np.nan)
        .median()
    )
    slope = np.polyfit(
        prec[PrecursorColumns.SCAN].to_numpy(float),
        prec[PrecursorColumns.RT].to_numpy(float),
        1,
    )[0]
    return float(step * slope * SECONDS_PER_MINUTE)


# ------------------------------------------------- PEAKS project database
# PEAKS stores each PSM as protobuf blobs in dia_psm_*.db. There is no schema, so
# the fields used here were identified from the data (PEAKS 13.6, 2026-10-29):
#
#   feature_dia.psm_details  field 20 -> field 6: 16-byte UUID = dia_psm.psm_id
#                            (all 52,652 exported features link one-to-one)
#   lib_proto     field 6  : fragment names, 'y4', 'b5', 'y9[2+]' (b/y only)
#   details_proto field 13 : 0-based indices into those names
#                 field 14 : float32 mass error per fragment, in Th - its IQR
#                            grows 4x from m/z 100 to 1,800 while the IQR in ppm
#                            stays flat at ~11, which is what an m/z error does
#   feature_proto field 7  : float32 RT grid (min) spanning the feature
#                 field 3  : one message per fragment: field 4 = float32
#                            chromatogram normalised to max 1, field 5 = its max
#
# Fragment m/z for the ppm conversion are computed from the sequence (b/y,
# monoisotopic), since the library m/z are stored compressed.
_PSM_DETAILS_FIELD, _PSM_ID_FIELD = 20, 6
_LIB_NAMES_FIELD = 6
_DETAILS_INDEX_FIELD, _DETAILS_ERROR_FIELD = 13, 14
_FEATURE_RT_FIELD, _FEATURE_FRAGMENTS_FIELD = 7, 3
_PROFILE_FIELD, _PROFILE_MAX_FIELD = 4, 5

_AA = {
    "G": 57.021464,
    "A": 71.037114,
    "S": 87.032028,
    "P": 97.052764,
    "V": 99.068414,
    "T": 101.047679,
    "C": 103.009185,
    "L": 113.084064,
    "I": 113.084064,
    "N": 114.042927,
    "D": 115.026943,
    "Q": 128.058578,
    "K": 128.094963,
    "E": 129.042593,
    "M": 131.040485,
    "H": 137.058912,
    "F": 147.068414,
    "R": 156.101111,
    "Y": 163.063329,
    "W": 186.079313,
}
# PEAKS writes modifications as '(+57.02)'. Two decimals is up to 0.005 Da, i.e.
# several ppm on a fragment, so common ones use exact masses; any other falls
# back to the printed value.
_MOD_MASS = {
    "+57.02": 57.021464,
    "+15.99": 15.994915,
    "+42.01": 42.010565,
    "+79.97": 79.966331,
    "+.98": 0.984016,
    "+0.98": 0.984016,
    "-17.03": -17.026549,
    "-18.01": -18.010565,
    "+43.01": 43.005814,
    "+14.02": 14.015650,
    "+28.03": 28.031300,
    "+114.04": 114.042927,
    "+229.16": 229.162932,
    "+304.21": 304.207146,
    "+203.08": 203.079373,
}
_PROTON, _H2O = 1.007276467, 18.010565
_FRAG = re.compile(r"([by])(\d+)(?:\[(\d)\+\])?$")
_RESIDUE = re.compile(r"(\([^)]*\))?([A-Z])(\(([^)]*)\))?")

# protobuf wire format
_WIRE_VARINT, _WIRE_FIXED64, _WIRE_LEN, _WIRE_FIXED32 = 0, 1, 2, 5
_VARINT_CONTINUE, _VARINT_PAYLOAD = 0x80, 0x7F


def _varint(b: bytes, i: int) -> tuple[int, int]:
    v = s = 0
    while True:
        x = b[i]
        i += 1
        v |= (x & _VARINT_PAYLOAD) << s
        s += 7
        if not x & _VARINT_CONTINUE:
            return v, i


def _packed_varints(b: bytes) -> list[int]:
    """Decode a packed repeated varint field."""
    out, i = [], 0
    while i < len(b):
        v, i = _varint(b, i)
        out.append(v)
    return out


def _pb(b: bytes) -> dict[int, list]:
    """Flat protobuf decode: {field: [values]}, length-delimited fields as bytes."""
    i, out = 0, defaultdict(list)
    while i < len(b):
        k, i = _varint(b, i)
        f, t = k >> 3, k & 7
        if t == _WIRE_VARINT:
            v, i = _varint(b, i)
        elif t == _WIRE_FIXED64:
            v = struct.unpack_from("<d", b, i)[0]
            i += 8
        elif t == _WIRE_FIXED32:
            v = struct.unpack_from("<f", b, i)[0]
            i += 4
        elif t == _WIRE_LEN:
            n, i = _varint(b, i)
            v = b[i : i + n]
            i += n
        else:
            raise ValueError(f"unsupported protobuf wire type {t}")
        out[f].append(v)
    return out


def _residue_masses(peptide: str) -> list[float]:
    """Residue masses with modifications; a leading '(+42.01)' goes on residue 1."""
    out = []
    for m in _RESIDUE.finditer(peptide):
        mass = _AA[m.group(2)]
        for mod in (m.group(1), m.group(4) and f"({m.group(4)})"):
            if mod:
                key = mod.strip("()")
                try:
                    mass += _MOD_MASS.get(key) or float(key)
                except ValueError:
                    return []  # unparseable modification: no fragment m/z
        out.append(mass)
    return out


@cache
def _parse_fragment(name: str) -> tuple[str, int, int] | None:
    """'y9[2+]' -> ('y', 9, 2); None for anything that is not a b/y ion."""
    m = _FRAG.match(name)
    return None if m is None else (m.group(1), int(m.group(2)), int(m.group(3) or 1))


def _fragment_mz(cum: np.ndarray, name: str) -> float:
    """b/y m/z from cumulative residue masses (cum[k] = mass of the first k residues)."""
    f = _parse_fragment(name)
    if f is None or cum.size < 2:  # noqa: PLR2004
        return np.nan  # not a b/y ion, or sequence not parseable
    kind, n, z = f
    if n >= cum.size:
        return np.nan
    mass = cum[n] if kind == "b" else cum[-1] - cum[-1 - n] + _H2O
    return (mass + z * _PROTON) / z


def _fragment_profile(b: bytes) -> tuple[np.ndarray | None, float]:
    """(chromatogram, its max) from one feature_proto field-3 message.

    Fast path for the fixed layout PEAKS writes (99.99% of messages):
    0a 01 xx | 12 01 xx | 1d f32 | 22 len <float32 x n> | 2d f32
    Anything else goes through the generic decoder.
    """
    if (
        len(b) > 13  # noqa: PLR2004
        and b[0] == 0x0A  # noqa: PLR2004
        and b[3] == 0x12  # noqa: PLR2004
        and b[6] == 0x1D  # noqa: PLR2004
        and b[11] == 0x22  # noqa: PLR2004
        and b[12] < 0x80  # noqa: PLR2004
    ):
        n = b[12]
        if len(b) == 18 + n and b[13 + n] == 0x2D:  # noqa: PLR2004
            return np.frombuffer(b, "<f4", n // 4, 13), struct.unpack_from(
                "<f", b, 14 + n
            )[0]
    g = _pb(b)
    if _PROFILE_FIELD in g and _PROFILE_MAX_FIELD in g:
        return np.frombuffer(g[_PROFILE_FIELD][0], "<f4"), g[_PROFILE_MAX_FIELD][0]
    return None, 0.0


def _half_height(rt: np.ndarray, y: np.ndarray) -> tuple[float, float]:
    """(FWHM, asymmetry) at half the apex height, crossings linearly interpolated.

    PEAKS stores each chromatogram only over its own integration window, and
    about a quarter of peaks do not fall to half height inside it. On such a
    side the window edge is taken as the crossing, so every peak gets a width
    (a slight underestimate for the truncated ones). An apex ON the window edge
    has no leading or trailing half: its asymmetry is undefined (NaN).
    """
    a = int(np.argmax(y))
    h = y[a] / 2
    if y[a] <= 0 or y.size < 2:  # noqa: PLR2004
        return np.nan, np.nan
    i = a
    while i > 0 and y[i - 1] > h:
        i -= 1
    left = (
        rt[i]
        if i == 0
        else rt[i - 1] + (h - y[i - 1]) / (y[i] - y[i - 1]) * (rt[i] - rt[i - 1])
    )
    j = a
    while j < y.size - 1 and y[j + 1] > h:
        j += 1
    right = (
        rt[j]
        if j == y.size - 1
        else rt[j] + (y[j] - h) / (y[j] - y[j + 1]) * (rt[j + 1] - rt[j])
    )
    lead, tail = rt[a] - left, right - rt[a]
    return right - left, (tail / lead if lead > 0 and tail > 0 else np.nan)


def _open_ro(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def _psm_ids_by_feature(
    projects: list[Path], feature_ids: pd.Series
) -> tuple[pd.Series, Path, set] | None:
    """(feature_id -> psm_id, project folder, fraction ids) for this run's features.

    Looks in every feature_*_dia.db of every project folder and keeps the one
    that holds the exported Feature Ids. Only this run's rows are decoded, so a
    project with many runs is not re-read in full for each of them.
    """
    ids = tuple(int(x) for x in pd.unique(feature_ids))
    for project in projects:
        for db in sorted(project.glob(FEATURE_DB_GLOB)):
            with closing(_open_ro(db)) as c:
                c.execute("create temp table want (feature_id integer primary key)")
                c.executemany(
                    "insert or ignore into want values (?)", ((i,) for i in ids)
                )
                rows = c.execute(
                    "select f.feature_id, f.fraction_id, f.psm_details from feature_dia f "
                    "join want w on f.feature_id = w.feature_id"
                ).fetchall()
            if not rows:
                continue
            out, fractions = {}, set()
            for fid, fraction, blob in rows:
                fractions.add(fraction)
                if not blob:
                    continue
                d = _pb(blob)
                if _PSM_DETAILS_FIELD in d:
                    sub = _pb(d[_PSM_DETAILS_FIELD][0])
                    if _PSM_ID_FIELD in sub:
                        out[fid] = str(uuid.UUID(bytes=sub[_PSM_ID_FIELD][0]))
            return pd.Series(out, name="psm_id"), project, fractions
    return None


def _psm_dbs(project: Path, fractions: set) -> list[Path]:
    """dia_psm_<task>__<fraction>.db files of this run's fraction(s); all if none match."""
    dbs = sorted(project.glob(PSM_DB_GLOB))
    keys = {str(f).replace("-", "_") for f in fractions if f}
    mine = [db for db in dbs if any(k in db.name for k in keys)]
    return mine or dbs


def _fragment_ppms(seq: str, det: bytes, lib: bytes) -> list[float]:
    """Mass error in ppm of every library fragment of one PSM."""
    d = _pb(det)
    if _DETAILS_INDEX_FIELD not in d or _DETAILS_ERROR_FIELD not in d:
        return []
    names = [n.decode() for n in _pb(lib)[_LIB_NAMES_FIELD]]
    cum = np.concatenate([[0.0], np.cumsum(_residue_masses(seq))])
    errs = np.frombuffer(d[_DETAILS_ERROR_FIELD][0], "<f4")
    return [
        e / _fragment_mz(cum, names[i]) * PPM_FACTOR
        for i, e in zip(_packed_varints(d[_DETAILS_INDEX_FIELD][0]), errs, strict=False)
        if i < len(names)
    ]


def _peak_shape(feat: bytes) -> tuple[float, float]:
    """(FWHM, asymmetry) of the summed fragment chromatogram of one PSM."""
    f = _pb(feat)
    if _FEATURE_RT_FIELD not in f:
        return np.nan, np.nan
    rt = (
        np.frombuffer(f[_FEATURE_RT_FIELD][0], "<f4").astype(float) * SECONDS_PER_MINUTE
    )
    tot = np.zeros(rt.size)
    for fr in f[_FEATURE_FRAGMENTS_FIELD]:
        prof, top = _fragment_profile(fr)
        if prof is not None and prof.size == rt.size:
            tot += prof * top
    return _half_height(rt, tot)


def _load_psm_details(
    run: _Run, prec: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    """Per-feature FWHM / asymmetry and per-fragment ppm errors from the project db.

    Returns (peaks, fragments): `peaks` indexed by export Feature Id with
    psm_id, fwhm_s and asymmetry; `fragments` one row per fragment with psm_id
    and ppm. None if the project folder or its databases are missing.
    """
    found = _psm_ids_by_feature(
        run.project_dirs, prec[PrecursorColumns.FEATURE_ID].dropna()
    )
    if found is None:
        return None
    psm_ids, project, fractions = found
    if psm_ids.empty:
        return None
    wanted = set(psm_ids)
    peaks, frag_psm, ppm = {}, [], []
    for db in _psm_dbs(project, fractions):
        with closing(_open_ro(db)) as c:
            for pid, seq, det, lib, feat in c.execute(
                "select psm_id, sequence, details_proto, lib_proto, feature_proto "
                "from dia_psm where decoy = 0"
            ):
                if pid not in wanted or pid in peaks:
                    continue
                ppms = _fragment_ppms(seq, det, lib)
                ppm.extend(ppms)
                frag_psm.extend([pid] * len(ppms))
                peaks[pid] = _peak_shape(feat)
    shape = pd.DataFrame.from_dict(
        peaks, orient="index", columns=["fwhm_s", "asymmetry"]
    )
    by_feature = psm_ids.to_frame().join(shape, on="psm_id")
    return by_feature, pd.DataFrame({"psm_id": frag_psm, "ppm": ppm})


def _peak_shape_population(prec: pd.DataFrame, peaks: pd.DataFrame) -> pd.DataFrame:
    """FWHM / asymmetry of every peak.

    One row per exported feature: several precursors can share a Feature Id, and a shared peak is one peak.
    """
    feats = prec[[PrecursorColumns.FEATURE_ID]].drop_duplicates()
    return feats.join(peaks, on=PrecursorColumns.FEATURE_ID).dropna(subset=["fwhm_s"])


def _fragment_ppm_population(
    prec: pd.DataFrame, peaks: pd.DataFrame, fragments: pd.DataFrame
) -> np.ndarray:
    """Fragment mass errors of every reported precursor.

    b/y ions only; any fragment whose m/z cannot be computed from the sequence has no error.
    """
    psm = (
        peaks["psm_id"]
        .reindex(prec[PrecursorColumns.FEATURE_ID].drop_duplicates())
        .dropna()
    )
    x = fragments.loc[fragments["psm_id"].isin(set(psm)), "ppm"].to_numpy(float)
    return x[np.isfinite(x)]


# ------------------------------------------------------------------ metrics
def _load_precursors(run: _Run) -> pd.DataFrame:
    df = pd.read_csv(run.precursor_file)
    df[PrecursorColumns.SEQUENCE] = strip_mods(df[PrecursorColumns.PEPTIDE])
    return df


def _protein_metrics(run: _Run) -> dict[str, Any]:
    out = {
        "n_protein_groups": np.nan,
        "n_protein_groups_2pep": np.nan,
        "protein_coverage_median": np.nan,
    }
    n_prec_col = ProteinColumns.N_PRECURSORS.format(run=run.run)
    if run.protein_file.exists():
        pg = pd.read_csv(run.protein_file)
        present = (
            pg[n_prec_col] > 0 if n_prec_col in pg else pd.Series(True, index=pg.index)  # noqa: FBT003
        )
        out["n_protein_groups"] = pg.loc[
            present, ProteinColumns.PROTEIN_GROUP
        ].nunique()
        cov = ProteinColumns.COVERAGE.format(run=run.run)
        if cov in pg:
            # one row per group: its Top (leading) protein
            lead = pg[present & pg[ProteinColumns.TOP]].drop_duplicates(
                ProteinColumns.PROTEIN_GROUP
            )
            out["protein_coverage_median"] = lead[cov].median()
    if run.protein_peptide_file.exists():
        pp = pd.read_csv(run.protein_peptide_file)
        if n_prec_col in pp:
            pp = pp[pp[n_prec_col] > 0]
        n_pep = pp.groupby(ProteinColumns.PROTEIN_GROUP)[
            ProteinColumns.PEPTIDE
        ].nunique()
        out["n_protein_groups_2pep"] = int((n_pep >= 2).sum())  # noqa: PLR2004
    return out


def _identification_metrics(prec: pd.DataFrame) -> dict[str, Any]:
    return {
        "n_peptides": prec[PrecursorColumns.PEPTIDE].nunique(),
        "n_sequences": prec[PrecursorColumns.SEQUENCE].nunique(),
        "n_precursors": len(
            prec[[PrecursorColumns.PEPTIDE, PrecursorColumns.CHARGE]].drop_duplicates()
        ),
        "median_score": prec[PrecursorColumns.SCORE].median(),
    }


def _chromatography_metrics(
    prec: pd.DataFrame, psm_details: tuple[pd.DataFrame, pd.DataFrame] | None
) -> dict[str, Any]:
    rt = prec[PrecursorColumns.RT]
    lo, hi = rt.quantile([P1, P99])
    m = {
        "rt_median_min": rt.median(),
        "elution_window_min": hi - lo,
        "cycle_time_s": cycle_time_s(prec),
    }
    if psm_details is not None:
        top = _peak_shape_population(prec, psm_details[0])
        a = top["asymmetry"].dropna()
        a = a[a > 0]
        m["fwhm_median_s"] = top["fwhm_s"].median()
        m["asymmetry_geomean"] = float(2 ** np.log2(a).mean())
        m["frac_tailing"] = (a > TAILING_CUTOFF).mean()
        m["points_per_fwhm"] = m["fwhm_median_s"] / m["cycle_time_s"]
    return m


def _signal_metrics(prec: pd.DataFrame) -> dict[str, Any]:
    """Signal metrics - unnormalised within a single-run search."""
    ms2 = prec[PrecursorColumns.MS2_AREA].dropna()
    ms1 = prec[PrecursorColumns.AREA].dropna()
    # a shared precursor lists several accessions; credit it to the first one
    prot = prec[PrecursorColumns.ACCESSION].astype(str).str.split(";").str[0]
    per_prot = (
        prec[PrecursorColumns.MS2_AREA].groupby(prot).sum().sort_values(ascending=False)
    )
    return {
        "total_ms2_area": ms2.sum(),
        "total_ms1_area": ms1.sum(),
        "frac_with_ms1": prec[PrecursorColumns.AREA].notna().mean(),
        "median_ms2_area": ms2.median(),
        "ms2_dynamic_range_log10": _dynamic_range(ms2),
        "ms1_dynamic_range_log10": _dynamic_range(ms1),
        "top10_protein_ms2_share": per_prot.head(TOP_N_PROTEINS).sum() / per_prot.sum(),
    }


def _mass_accuracy_metrics(
    prec: pd.DataFrame, psm_details: tuple[pd.DataFrame, pd.DataFrame] | None
) -> dict[str, Any]:
    ppm = measured_ppm(prec)
    m = {"ms1_ppm_median": ppm.median(), "ms1_ppm_mad": _mad(ppm)}
    if psm_details is not None:
        frag_ppm = _fragment_ppm_population(prec, *psm_details)
        m["ms2_ppm_median"] = float(np.median(frag_ppm))
        m["ms2_ppm_mad"] = _mad(frag_ppm)
    return m


def _chemistry_metrics(prec: pd.DataFrame) -> dict[str, Any]:
    """Sample prep / chemistry, at peptide level (one row per modified peptide)."""
    pep = prec.drop_duplicates(PrecursorColumns.PEPTIDE)
    has_m = pep[PrecursorColumns.SEQUENCE].str.contains("M")
    ox = pep[PrecursorColumns.PTM].fillna("").str.contains("Oxidation")
    m = {
        "missed_cleavage_rate": (
            missed_cleavages(pep[PrecursorColumns.SEQUENCE]) > 0
        ).mean(),
        "met_oxidation_rate": ox[has_m].mean() if has_m.any() else np.nan,
        "peptide_length_median": pep[PrecursorColumns.SEQUENCE].str.len().median(),
    }
    z = prec[PrecursorColumns.CHARGE].clip(upper=MAX_CHARGE_BIN)
    for k in range(1, MAX_CHARGE_BIN + 1):
        m[f"frac_z{k}"] = (z == k).mean()
    return m


def calc_peaks_metrics(output_directory: Path) -> dict[str, Any]:
    """Calculate PEAKS metrics, NaN for the ones that could not be calculated."""
    run = _find_run(output_directory)
    prec = _load_precursors(run)
    psm_details = _load_psm_details(run, prec)
    if psm_details is None:
        logging.warning(
            "No PEAKS project database found, peak shape and MS2 mass error metrics are NaN"
        )

    m = _protein_metrics(run)
    m |= _identification_metrics(prec)
    m |= _chromatography_metrics(prec, psm_details)
    m |= _signal_metrics(prec)
    m |= _mass_accuracy_metrics(prec, psm_details)
    m |= _chemistry_metrics(prec)
    return {k: m.get(k, np.nan) for k in METRICS}
