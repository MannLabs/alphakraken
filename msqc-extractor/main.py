"""Calculate performance metrics and compile TIC data for Thermo and Bruker raw files."""

import sys

import alphatims.bruker
import alphatims.utils
import numpy as np
import pandas as pd
from alpharaw import thermo

# Based on https://github.com/MannLabs/alpharaw/blob/main/docs/tutorials/ms_methods.ipynb


def _tic_for_spectrum_df(
    spectrum_df: pd.DataFrame, peak_df: pd.DataFrame
) -> pd.DataFrame:
    """Calculate the TIC for each spectrum in the spectrum_df.

    Parameters
    ----------
    spectrum_df : pd.DataFrame
        Spectrum data for specific MS level
    peak_df : pd.DataFrame
        Peak data

    Returns
    -------
        pd.DataFrame
            DataFrame with the TIC for each spectrum

    """
    tic_data = []
    for spec_idx, peak_start_idx, peak_stop_idx in zip(
        spectrum_df["spec_idx"],
        spectrum_df["peak_start_idx"],
        spectrum_df["peak_stop_idx"],
    ):
        tic_data.append(
            {
                "spec_idx": spec_idx,
                "tic": peak_df.iloc[peak_start_idx:peak_stop_idx]["intensity"].sum(),
            }
        )

    return pd.DataFrame(tic_data)


def calculate_thermo_metrics(
    raw_file: thermo.ThermoRawData,
) -> tuple[dict, pd.DataFrame]:
    """Calculate performance metrics and compile TIC data for Thermo raw file.

    Parameters
    ----------
    raw_file : thermo.ThermoRawData
        Loaded Thermo raw file object

    Returns
    -------
    tuple[dict, pd.DataFrame]
        (dict_ms_metrics, combined_tic_df) containing metrics and TIC data

    """
    dict_ms_metrics = {}
    all_tic_data = []

    for ms_level in raw_file.spectrum_df["ms_level"].unique():
        level_data = raw_file.spectrum_df[raw_file.spectrum_df["ms_level"] == ms_level]
        tic_df = _tic_for_spectrum_df(level_data, raw_file.peak_df)

        injection_times = level_data["injection_time"]
        dict_ms_metrics[f"ms{ms_level}_median_injection_time"] = np.median(
            injection_times
        )

        num_scans = level_data["precursor_mz"].nunique()
        dict_ms_metrics[f"ms{ms_level}_scans"] = num_scans

        dict_ms_metrics[f"ms{ms_level}_median_tic"] = np.median(tic_df["tic"])

        tic_df["ms_level"] = ms_level
        all_tic_data.append(tic_df)

    combined_tic_df = pd.concat(all_tic_data, ignore_index=True)

    return dict_ms_metrics, combined_tic_df


def _filter_bruker_data(
    data: alphatims.bruker.TimsTOF,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Filter Bruker data into MS1 and MS2 components.

    Parameters
    ----------
    data : alphatims.bruker.TimsTOF
        Loaded Bruker timsTOF data

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (chrom_ms1, chrom_ms2) DataFrames with chromatographic data
        where MsMsType==0 indicates MS1 scans and MsMsType!=0 indicates MS2 scans

    """
    chrom_ms1 = data.frames.query("MsMsType == 0")[
        ["Time", "SummedIntensities", "MaxIntensity"]
    ]
    # Convert retention times from seconds to minutes for better readability of plots
    chrom_ms1["RT"] = chrom_ms1["Time"] / 60

    chrom_ms2 = data.frames.query("MsMsType != 0")[
        ["Time", "SummedIntensities", "MaxIntensity"]
    ]
    chrom_ms2["RT"] = chrom_ms2["Time"] / 60

    return chrom_ms1, chrom_ms2


def calculate_bruker_metrics(
    data: alphatims.bruker.TimsTOF,
) -> tuple[dict, pd.DataFrame]:
    """Calculate performance metrics and compile TIC data for Bruker timsTOF data.

    Parameters
    ----------
    data : alphatims.bruker.TimsTOF
        Loaded Bruker timsTOF data

    Returns
    -------
    tuple
        (dict_ms_metrics, combined_tic_df) containing metrics and TIC data

    """
    dict_ms_metrics = {}

    dict_ms_metrics["ms2_scans"] = len(set(data.precursor_indices))

    chrom_ms1, chrom_ms2 = _filter_bruker_data(data)

    dict_ms_metrics["ms1_median_tic"] = np.median(chrom_ms1["SummedIntensities"])
    dict_ms_metrics["ms2_median_tic"] = np.median(chrom_ms2["SummedIntensities"])

    chrom_ms1["ms_level"] = 1
    chrom_ms2["ms_level"] = 2
    combined_tic_df = pd.concat([chrom_ms1, chrom_ms2], ignore_index=True)
    combined_tic_df = combined_tic_df.rename(
        columns={"Time": "spec_idx", "SummedIntensities": "tic"}
    )

    return dict_ms_metrics, combined_tic_df


if __name__ == "__main__":
    if len(sys.argv) != 4:  # noqa: PLR2004
        print("Usage: python main.py <raw_file_path> <output_path> <num_threads>")  # noqa: T201
        sys.exit(0)

    raw_file_path = sys.argv[1]
    output_path = sys.argv[2]
    num_threads = int(sys.argv[3])

    print(f"Starting MSQC extraction {raw_file_path} {output_path} {num_threads=}")  #  noqa: T201

    if raw_file_path.endswith(".raw"):
        auxiliary_items = ["injection_time", "faims_cv"]
        raw_file = thermo.ThermoRawData(auxiliary_items=auxiliary_items)
        raw_file.load_raw(raw_file_path)
        ms_metrics, combined_tic_df = calculate_thermo_metrics(raw_file)

    elif raw_file_path.endswith(".d"):
        alphatims.utils.set_threads(num_threads)

        data = alphatims.bruker.TimsTOF(raw_file_path)
        ms_metrics, combined_tic_df = calculate_bruker_metrics(data)
    else:
        print("Unsupported file format. Please provide a .raw or .d file.")  #  noqa: T201
        sys.exit(0)

    combined_tic_df.to_csv(f"{output_path}/msqc_tic.tsv", sep="\t")
    pd.DataFrame(ms_metrics, index=[0]).to_csv(
        f"{output_path}/msqc_results.tsv", sep="\t"
    )

    print("Finished MSQC extraction!")  #  noqa: T201
