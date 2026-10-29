"""Per-run QC metrics from PEAKS 13.x CLI (DIA db search) output.

Returns ONE NUMBER PER METRIC per run, for tracking instrument / LC / prep
performance over time. Two sources, both inside the PEAKS output folder:

- `exports/` - `<run>.dia_db.precursor.csv`, `dia_db.proteins.csv`,
  `dia_db.protein-peptides.csv`: identifications, RT, areas, MS1 mass error,
  chemistry.
- the PEAKS project folder next to it (`<run>/`, SQLite) - the per-fragment
  chromatograms and per-fragment mass errors PEAKS stores for every PSM but does
  not export. They give the FWHM, the half-height asymmetry and the MS2 mass
  error. If the project folder is missing those metrics are NaN.

Needs only numpy, pandas (and matplotlib for the plot) - no raw file.

    python peaks_run_qc.py <root>                     # finds every run below <root>
    python peaks_run_qc.py <root> --out qc.csv --figures

<root> can be one PEAKS output folder or a parent holding many (e.g. one
alphakraken output folder per run); every `*.dia_db.precursor.csv` below it is
one run. A run is identified by (search, run), search being the name of the
folder holding `exports/`, so two searches of the same file stay separate rows.

Metric definitions are in METRICS below and printed by `describe()`.
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import struct
import uuid
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

PRECURSOR_SUFFIX = ".precursor.csv"  # exports/<run>.<search type>.precursor.csv
TAILING_CUTOFF = 1.5  # half-height asymmetry above this counts as a tailing peak

# name -> (unit, definition). Order here is the column order of the output.
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
    "frac_z4": ("", "fraction at charge >=4"),
}


def describe() -> pd.DataFrame:
    return pd.DataFrame(
        [(k, u, d) for k, (u, d) in METRICS.items()],
        columns=["metric", "unit", "definition"],
    )


# ---------------------------------------------------------------- discovery
_PRECURSOR_FILE = re.compile(
    r"^(?P<run>.+)\.(?P<search_type>[^.]+)" + re.escape(PRECURSOR_SUFFIX) + "$"
)


def _project_dirs(search_dir: Path) -> list[Path]:
    """PEAKS project folders (SQLite databases) next to `exports/`.

    The folder is named after the PEAKS project, which for a CLI run is the run
    name but for a multi-run project is not - so find it by content.
    """
    return [
        d
        for d in sorted(search_dir.iterdir())
        if d.is_dir() and any(d.glob("feature_*_dia.db"))
    ]


def find_runs(root) -> pd.DataFrame:
    """One row per run below `root`: search, run, and the paths of its files.

    A run is a `<run>.<search type>.precursor.csv` (e.g. `.dia_db.`); the
    protein tables of the same search type are taken from the same folder.
    """
    rows = []
    for p in sorted(Path(root).rglob("*.precursor.csv")):
        m = _PRECURSOR_FILE.match(p.name)
        if m is None:
            continue
        exports, kind = p.parent, m["search_type"]
        projects = _project_dirs(exports.parent)
        rows.append(
            dict(
                search=exports.parent.name,
                run=m["run"],
                precursor_file=p,
                protein_file=exports / f"{kind}.proteins.csv",
                protein_peptide_file=exports / f"{kind}.protein-peptides.csv",
                project_dirs=projects,
            )
        )
    if not rows:
        raise FileNotFoundError(f"no *.<search type>{PRECURSOR_SUFFIX} below {root}")
    return pd.DataFrame(rows)


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


def _mad(x) -> float:
    x = np.asarray(x, float)
    x = x[np.isfinite(x)]
    if x.size < 2:
        return np.nan
    return float(np.median(np.abs(x - np.median(x))))


def _dynamic_range(area: pd.Series) -> float:
    """log10 P1-P99 span; the full min-max is set by single outlier features."""
    a = area[area > 0]
    if len(a) < 100:
        return np.nan
    lo, hi = np.log10(a).quantile([0.01, 0.99])
    return float(hi - lo)


def measured_ppm(prec: pd.DataFrame) -> pd.Series:
    """Precursor mass error of every precursor that has one (MS1 feature).

    Note: 11% of values are exactly 0.0, against ~1% expected from the
    neighbouring bins - PEAKS appears to write 0.0 for some unmeasured errors.
    They are kept, so the median sits near 0 and the MAD is slightly narrowed.
    """
    return prec["ppm"].dropna()


def _scan_lists(col: pd.Series) -> pd.Series:
    return col.astype(str).str.split(":").map(lambda v: np.asarray(v, dtype=np.int64))


def cycle_time_s(prec: pd.DataFrame) -> float:
    """Cycle time from the export alone.

    The scan numbers of one precursor's MS2 scans step by exactly one cycle (80
    windows + 1 MS1 = 81 here), and apex scan number vs apex RT is linear, so
    cycle = step x minutes-per-scan.
    """
    step = (
        _scan_lists(prec["MS2 Scans"].head(5000))
        .map(lambda v: np.median(np.diff(v)) if v.size > 1 else np.nan)
        .median()
    )
    slope = np.polyfit(prec["Scan"].to_numpy(float), prec["RT"].to_numpy(float), 1)[0]
    return float(step * slope * 60)


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
_AA = dict(
    G=57.021464,
    A=71.037114,
    S=87.032028,
    P=97.052764,
    V=99.068414,
    T=101.047679,
    C=103.009185,
    L=113.084064,
    I=113.084064,
    N=114.042927,
    D=115.026943,
    Q=128.058578,
    K=128.094963,
    E=129.042593,
    M=131.040485,
    H=137.058912,
    F=147.068414,
    R=156.101111,
    Y=163.063329,
    W=186.079313,
)
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


def _varint(b, i):
    v = s = 0
    while True:
        x = b[i]
        i += 1
        v |= (x & 0x7F) << s
        s += 7
        if not x & 0x80:
            return v, i


def _pb(b) -> dict:
    """Flat protobuf decode: {field: [values]}, length-delimited fields as bytes."""
    i, out = 0, defaultdict(list)
    while i < len(b):
        k, i = _varint(b, i)
        f, t = k >> 3, k & 7
        if t == 0:
            v, i = _varint(b, i)
        elif t == 1:
            v = struct.unpack_from("<d", b, i)[0]
            i += 8
        elif t == 5:
            v = struct.unpack_from("<f", b, i)[0]
            i += 4
        elif t == 2:
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


_FRAG_NAMES: dict = {}


def _parse_fragment(name: str):
    """'y9[2+]' -> ('y', 9, 2); None for anything that is not a b/y ion. Cached."""
    if name not in _FRAG_NAMES:
        m = _FRAG.match(name)
        _FRAG_NAMES[name] = (
            None if m is None else (m.group(1), int(m.group(2)), int(m.group(3) or 1))
        )
    return _FRAG_NAMES[name]


def _fragment_mz(cum: np.ndarray, name: str) -> float:
    """b/y m/z from cumulative residue masses (cum[k] = mass of the first k residues)."""
    f = _parse_fragment(name)
    if f is None or cum.size < 2:
        return np.nan  # not a b/y ion, or sequence not parseable
    kind, n, z = f
    if n >= cum.size:
        return np.nan
    mass = cum[n] if kind == "b" else cum[-1] - cum[-1 - n] + _H2O
    return (mass + z * _PROTON) / z


def _fragment_profile(b: bytes):
    """(chromatogram, its max) from one feature_proto field-3 message.

    Fast path for the fixed layout PEAKS writes (99.99% of messages):
    0a 01 xx | 12 01 xx | 1d f32 | 22 len <float32 x n> | 2d f32
    Anything else goes through the generic decoder.
    """
    if (
        len(b) > 13
        and b[0] == 0x0A
        and b[3] == 0x12
        and b[6] == 0x1D
        and b[11] == 0x22
        and b[12] < 0x80
    ):
        n = b[12]
        if len(b) == 18 + n and b[13 + n] == 0x2D:
            return np.frombuffer(b, "<f4", n // 4, 13), struct.unpack_from(
                "<f", b, 14 + n
            )[0]
    g = _pb(b)
    if 4 in g and 5 in g:
        return np.frombuffer(g[4][0], "<f4"), g[5][0]
    return None, 0.0


def _half_height(rt: np.ndarray, y: np.ndarray):
    """(FWHM, asymmetry) at half the apex height, crossings linearly interpolated.

    PEAKS stores each chromatogram only over its own integration window, and
    about a quarter of peaks do not fall to half height inside it. On such a
    side the window edge is taken as the crossing, so every peak gets a width
    (a slight underestimate for the truncated ones). An apex ON the window edge
    has no leading or trailing half: its asymmetry is undefined (NaN).
    """
    a = int(np.argmax(y))
    h = y[a] / 2
    if y[a] <= 0 or y.size < 2:
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


def _open_ro(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def _psm_ids_by_feature(projects, feature_ids):
    """(feature_id -> psm_id, project folder, fraction ids) for this run's features.

    Looks in every feature_*_dia.db of every project folder and keeps the one
    that holds the exported Feature Ids. Only this run's rows are decoded, so a
    project with many runs is not re-read in full for each of them.
    """
    ids = tuple(int(x) for x in pd.unique(feature_ids))
    for project in projects:
        for db in sorted(Path(project).glob("feature_*_dia.db")):
            with _open_ro(db) as c:
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
                if 20 in d:
                    sub = _pb(d[20][0])
                    if 6 in sub:
                        out[fid] = str(uuid.UUID(bytes=sub[6][0]))
            return pd.Series(out, name="psm_id"), Path(project), fractions
    return pd.Series(dtype=object, name="psm_id"), None, set()


def _psm_dbs(project: Path, fractions) -> list[Path]:
    """dia_psm_<task>__<fraction>.db files of this run's fraction(s); all if none match."""
    dbs = sorted(project.glob("dia_psm_*.db"))
    keys = {str(f).replace("-", "_") for f in fractions if f}
    mine = [db for db in dbs if any(k in db.name for k in keys)]
    return mine or dbs


def load_psm_details(run_row, prec: pd.DataFrame):
    """Per-feature FWHM / asymmetry and per-fragment ppm errors from the project db.

    Returns (peaks, fragments): `peaks` indexed by export Feature Id with
    psm_id, fwhm_s and asymmetry; `fragments` one row per fragment with psm_id
    and ppm. (None, None) if the project folder or its databases are missing.
    """
    psm_ids, project, fractions = _psm_ids_by_feature(
        run_row["project_dirs"], prec["Feature Id"].dropna()
    )
    if psm_ids.empty:
        return None, None
    wanted = set(psm_ids)
    peaks, frag_psm, ppm = {}, [], []
    for db in _psm_dbs(project, fractions):
        with _open_ro(db) as c:
            for pid, seq, det, lib, feat in c.execute(
                "select psm_id, sequence, details_proto, lib_proto, feature_proto "
                "from dia_psm where decoy = 0"
            ):
                if pid not in wanted or pid in peaks:
                    continue
                d, f = _pb(det), _pb(feat)
                # fragment mass errors
                names = [n.decode() for n in _pb(lib)[6]]
                if 13 in d and 14 in d:
                    cum = np.concatenate([[0.0], np.cumsum(_residue_masses(seq))])
                    errs = np.frombuffer(d[14][0], "<f4")
                    for i, e in zip(d[13][0], errs):
                        if i < len(names):
                            ppm.append(e / _fragment_mz(cum, names[i]) * 1e6)
                            frag_psm.append(pid)
                # summed fragment chromatogram
                if 7 in f:
                    rt = np.frombuffer(f[7][0], "<f4").astype(float) * 60
                    tot = np.zeros(rt.size)
                    for fr in f[3]:
                        prof, top = _fragment_profile(fr)
                        if prof is not None and prof.size == rt.size:
                            tot += prof * top
                    peaks[pid] = _half_height(rt, tot)
                else:
                    peaks[pid] = (np.nan, np.nan)
    shape = pd.DataFrame.from_dict(
        peaks, orient="index", columns=["fwhm_s", "asymmetry"]
    )
    by_feature = psm_ids.to_frame().join(shape, on="psm_id")
    return by_feature, pd.DataFrame(dict(psm_id=frag_psm, ppm=ppm))


def peak_shape_population(prec: pd.DataFrame, peaks: pd.DataFrame) -> pd.DataFrame:
    """FWHM / asymmetry of every peak: one row per exported feature (several
    precursors can share a Feature Id, and a shared peak is one peak).
    """
    feats = prec[["Feature Id"]].drop_duplicates()
    return feats.join(peaks, on="Feature Id").dropna(subset=["fwhm_s"])


def fragment_ppm_population(
    prec: pd.DataFrame, peaks: pd.DataFrame, fragments: pd.DataFrame
) -> np.ndarray:
    """Fragment mass errors of every reported precursor (b/y ions; any fragment
    whose m/z cannot be computed from the sequence has no error).
    """
    psm = peaks["psm_id"].reindex(prec["Feature Id"].drop_duplicates()).dropna()
    x = fragments.loc[fragments["psm_id"].isin(set(psm)), "ppm"].to_numpy(float)
    return x[np.isfinite(x)]


# ------------------------------------------------------------------ loading
def load_precursors(run_row) -> pd.DataFrame:
    df = pd.read_csv(run_row["precursor_file"])
    df["sequence"] = strip_mods(df["Peptide"])
    return df


# ------------------------------------------------------------------ metrics
def protein_metrics(run_row) -> dict:
    run = run_row["run"]
    out = dict(
        n_protein_groups=np.nan,
        n_protein_groups_2pep=np.nan,
        protein_coverage_median=np.nan,
    )
    pf = Path(run_row["protein_file"])
    if pf.exists():
        pg = pd.read_csv(pf)
        col = f"#Precursors {run}"
        present = pg[col] > 0 if col in pg else pd.Series(True, index=pg.index)
        out["n_protein_groups"] = pg.loc[present, "Protein Group"].nunique()
        cov = f"Coverage(%) {run}"
        if cov in pg:
            # one row per group: its Top (leading) protein
            lead = pg[present & pg["Top"]].drop_duplicates("Protein Group")
            out["protein_coverage_median"] = lead[cov].median()
    ppf = Path(run_row["protein_peptide_file"])
    if ppf.exists():
        pp = pd.read_csv(ppf)
        col = f"#Precursors {run}"
        if col in pp:
            pp = pp[pp[col] > 0]
        n_pep = pp.groupby("Protein Group")["Peptide"].nunique()
        out["n_protein_groups_2pep"] = int((n_pep >= 2).sum())
    return out


def run_metrics(run_row) -> dict:
    prec = load_precursors(run_row)
    peaks, fragments = load_psm_details(run_row, prec)
    m = dict(search=run_row["search"], run=run_row["run"])
    m.update(protein_metrics(run_row))

    # identifications
    m["n_peptides"] = prec["Peptide"].nunique()
    m["n_sequences"] = prec["sequence"].nunique()
    m["n_precursors"] = len(prec[["Peptide", "z"]].drop_duplicates())
    m["median_score"] = prec["-10LgP"].median()

    # chromatography
    rt = prec["RT"]
    m["rt_median_min"] = rt.median()
    lo, hi = rt.quantile([0.01, 0.99])
    m["elution_window_min"] = hi - lo
    m["cycle_time_s"] = cycle_time_s(prec)
    if peaks is not None:
        top = peak_shape_population(prec, peaks)
        a = top["asymmetry"].dropna()
        a = a[a > 0]
        m["fwhm_median_s"] = top["fwhm_s"].median()
        m["asymmetry_geomean"] = float(2 ** np.log2(a).mean())
        m["frac_tailing"] = (a > TAILING_CUTOFF).mean()
        m["points_per_fwhm"] = m["fwhm_median_s"] / m["cycle_time_s"]

    # signal - unnormalised within a single-run search
    ms2 = prec["MS2 Area"].dropna()
    ms1 = prec["Area"].dropna()
    m["total_ms2_area"] = ms2.sum()
    m["total_ms1_area"] = ms1.sum()
    m["frac_with_ms1"] = prec["Area"].notna().mean()
    m["median_ms2_area"] = ms2.median()
    m["ms2_dynamic_range_log10"] = _dynamic_range(ms2)
    m["ms1_dynamic_range_log10"] = _dynamic_range(ms1)
    # a shared precursor lists several accessions; credit it to the first one
    prot = prec["Accession"].astype(str).str.split(";").str[0]
    per_prot = prec["MS2 Area"].groupby(prot).sum().sort_values(ascending=False)
    m["top10_protein_ms2_share"] = per_prot.head(10).sum() / per_prot.sum()

    # mass accuracy
    ppm = measured_ppm(prec)
    m["ms1_ppm_median"] = ppm.median()
    m["ms1_ppm_mad"] = _mad(ppm)
    if peaks is not None:
        frag_ppm = fragment_ppm_population(prec, peaks, fragments)
        m["ms2_ppm_median"] = float(np.median(frag_ppm))
        m["ms2_ppm_mad"] = _mad(frag_ppm)

    # prep / chemistry, at peptide level (one row per modified peptide)
    pep = prec.drop_duplicates("Peptide")
    m["missed_cleavage_rate"] = (missed_cleavages(pep["sequence"]) > 0).mean()
    has_m = pep["sequence"].str.contains("M")
    ox = pep["PTM"].fillna("").str.contains("Oxidation")
    m["met_oxidation_rate"] = ox[has_m].mean() if has_m.any() else np.nan
    m["peptide_length_median"] = pep["sequence"].str.len().median()
    z = prec["z"].clip(upper=4)
    for k in (1, 2, 3, 4):
        m[f"frac_z{k}"] = (z == k).mean()

    cols = ["search", "run"] + list(METRICS)
    return {k: m.get(k, np.nan) for k in cols}


def collect(root) -> pd.DataFrame:
    runs = find_runs(root)
    rows = []
    for _, r in runs.iterrows():
        print(f"[peaks_run_qc] {r['search']} / {r['run']}")
        rows.append(run_metrics(r))
    return pd.DataFrame(rows)


# --------------------------------------------------------------------- plot
def plot_run_distributions(run_row, out_dir=None, colors=("#FA7A61", "#78B8E2")):
    """2x2 panel of the distributions behind the summary numbers for one run.

    Saved as PDF + PNG (300 dpi) + _sourcedata.csv when `out_dir` is given.
    """
    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "svg.fonttype": "none",
            "font.family": "sans-serif",
            "font.sans-serif": ["DejaVu Sans"],
            "axes.labelsize": 10,
            "xtick.labelsize": 10,
            "ytick.labelsize": 10,
            "axes.titlesize": 11,
            "axes.titleweight": "bold",
            "axes.spines.top": False,
            "axes.spines.right": False,
        }
    )
    prec = load_precursors(run_row)
    peaks, fragments = load_psm_details(run_row, prec)
    top = peak_shape_population(prec, peaks) if peaks is not None else None
    frag_ppm = (
        fragment_ppm_population(prec, peaks, fragments) if peaks is not None else None
    )

    fig, axes = plt.subplots(2, 2, figsize=(8, 8))
    src = []

    def hist(ax, v, bins, label, color, log2=False):
        v = np.asarray(v, float)
        v = v[np.isfinite(v)]
        if log2:
            v = np.log2(v[v > 0])
        counts, edges = np.histogram(v, bins=bins)
        centres = (edges[:-1] + edges[1:]) / 2
        (line,) = ax.plot(centres, counts, color=color, lw=2.2)
        ax.fill_between(centres, counts, color=color, alpha=0.18, lw=0)
        med = np.median(v)
        ax.axvline(med, color=color, ls="--", lw=1)
        shown = 2 ** v.mean() if log2 else med  # asymmetry reports the geometric mean
        src.append(
            pd.DataFrame(
                dict(
                    panel=ax.get_title(), series=label, bin_centre=centres, count=counts
                )
            )
        )
        return f"{label}  {shown:.3g}  (n={len(v):,})", np.diff(edges).mean(), line

    def key(ax, handles, labels, title=None, log=False):
        # headroom above the curves so the readout never sits on the data
        lo, hi = ax.get_ylim()
        ax.set_ylim(lo, hi * (30 if log else 1.3))
        ax.legend(
            handles,
            labels,
            frameon=False,
            fontsize=9,
            loc="upper right",
            title=title,
            title_fontsize=9,
            handlelength=0.9,
            handletextpad=0.4,
            borderpad=0.2,
        )

    ax = axes[0, 0]
    ax.set_title("precursor RT")
    lab, w, h = hist(ax, prec["RT"], 60, "median", colors[0])
    key(ax, [h], [lab])
    ax.set_xlabel("apex RT [min]")
    ax.set_ylabel(f"precursors per bin (bin {w:.2g} min)")

    ax = axes[0, 1]
    ax.set_title("FWHM")
    if top is not None:
        lab, w, h = hist(ax, top["fwhm_s"], np.linspace(0, 6, 61), "median", colors[0])
        key(ax, [h], [lab])
        ax.set_ylabel(f"precursors per bin (bin {w:.2g} s)")
    ax.set_xlabel("FWHM [s]")

    ax = axes[1, 0]
    ax.set_title("half-height asymmetry")
    if top is not None:
        lab, w, h = hist(
            ax,
            top["asymmetry"],
            np.linspace(-2.5, 2.5, 51),
            "geo. mean",
            colors[0],
            log2=True,
        )
        key(ax, [h], [lab])
        ticks = [0.25, 0.5, 1, 2, 4]
        ax.set_xticks(np.log2(ticks), [f"{t:g}" for t in ticks])
        ax.set_ylabel("precursors per bin (bin 0.1 log2)")
    ax.set_xlabel("trailing / leading half-width")

    ax = axes[1, 1]
    ax.set_title("mass error")
    bins = np.linspace(-30, 30, 121)
    series = [hist(ax, measured_ppm(prec), bins, "MS1", colors[0])]
    if frag_ppm is not None:
        series.append(hist(ax, frag_ppm, bins, "MS2", colors[1]))
    ax.set_yscale("log")
    key(ax, [s[2] for s in series], [s[0] for s in series], title="median", log=True)
    ax.set_xlabel("error [ppm]")
    ax.set_ylabel("ions per bin (bin 0.5 ppm)")

    fig.suptitle(f"{run_row['run']} | {run_row['search']}", fontsize=9)
    fig.tight_layout()
    if out_dir is not None:
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        stem = out / f"{run_row['run']}_qc_distributions"
        fig.savefig(stem.with_suffix(".pdf"))
        fig.savefig(stem.with_suffix(".png"), dpi=300)
        pd.concat(src).to_csv(f"{stem}_sourcedata.csv", index=False)
    return fig


# ---------------------------------------------------------------------- CLI
def main(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("root", help="PEAKS output folder, or a parent of many")
    ap.add_argument(
        "--out", default=None, help="metrics CSV (default <root>/peaks_run_qc.csv)"
    )
    ap.add_argument(
        "--figures",
        action="store_true",
        help="also write one distribution panel per run to <root>/qc_figures",
    )
    args = ap.parse_args(argv)

    root = Path(args.root)
    table = collect(root)
    out = Path(args.out) if args.out else root / "peaks_run_qc.csv"
    table.to_csv(out, index=False)
    with pd.option_context(
        "display.max_rows",
        None,
        "display.width",
        200,
        "display.float_format",
        "{:.6g}".format,
    ):
        print(table.set_index(["search", "run"]).T.to_string())
    print(f"\nwritten: {out}")
    if args.figures:
        for _, r in find_runs(root).iterrows():
            plot_run_distributions(r, out_dir=root / "qc_figures")
        print(f"figures: {root / 'qc_figures'}")
    return table


if __name__ == "__main__":
    main()
