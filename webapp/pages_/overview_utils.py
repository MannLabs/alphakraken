"""Utility functions for the overview page with no Streamlit dependencies."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
import yaml
from service.components import get_display_time

from shared.db.models import TERMINAL_STATUSES


@dataclass
class Column:
    """Data class for information on how to display a column information."""

    name: str
    # hide column in table
    hide: bool = False
    # move column to end of table
    at_end: bool = False
    # color gradient in table: None (no gradient), "green_is_high" (green=high, red=low), "red_is_high" (red=high, green=low)
    color_gradient: str | None = None
    # show as plot
    plot: bool = False
    # use log scale for plot
    log_scale: bool = False
    # alternative names in the database
    alternative_names: list[str] | None = None
    # optional plot
    plot_optional: bool = False


def _load_columns_from_yaml() -> tuple[Column, ...]:
    """Load column configuration from YAML file."""
    columns_config_file_path = Path(__file__).parent / ".." / "columns_config.yaml"

    with columns_config_file_path.open() as f:
        columns_config = yaml.safe_load(f)

    return tuple(
        [
            Column(
                name=column["name"],
                hide=column.get("hide"),
                at_end=column.get("at_end"),
                color_gradient=column.get("color_gradient"),
                plot=column.get("plot"),
                log_scale=column.get("log_scale"),
                alternative_names=column.get("alternative_names"),
                plot_optional=column.get("plot_optional"),
            )
            for column in columns_config["columns"]
        ]
    )


def _harmonize_df(df: pd.DataFrame, columns: tuple[Column, ...]) -> pd.DataFrame:
    """Harmonize the DataFrame by mapping all alternative names to their current ones."""
    names_mapping = {
        alternative_name: column.name
        for column in columns
        if column.alternative_names
        for alternative_name in column.alternative_names  # type: ignore[not-iterable]
        if column.alternative_names is not None
    }
    df = df.rename(columns=names_mapping)

    if "gradient_length" in df.columns:
        df["gradient_length"] = df["gradient_length"].apply(lambda x: round(x, 1))

    # map all columns of the same name to the first one, assuming that not more than one of the values are filled
    return df.groupby(axis=1, level=0).first()


def _get_column_order(df: pd.DataFrame, columns: tuple[Column, ...]) -> list[str]:
    """Get column order."""
    known_columns = [column.name for column in columns if column.name in df.columns]
    columns_at_end = [column.name for column in columns if column.at_end] + [
        col for col in df.columns if col.endswith("_std")
    ]
    columns_to_hide = [column.name for column in columns if column.hide]

    return (
        [col for col in known_columns if col not in columns_at_end + columns_to_hide]
        + [
            col
            for col in df.columns
            if col not in known_columns + columns_at_end + columns_to_hide
        ]
        + columns_at_end
    )


def _filter_valid_columns(columns: list[str], df: pd.DataFrame) -> list[str]:
    """Filter out `columns` that are not in the `df`."""
    return [col for col in columns if col in df.columns]


@st.cache_data
def df_to_csv(df: pd.DataFrame) -> str:
    """Convert a DataFrame to a CSV string."""
    return df.to_csv().encode("utf-8")


def _add_eta(df: pd.DataFrame, now: datetime, lag_time: float) -> pd.Series:
    """Return the "ETA" column for the dataframe."""
    # TODO: this would become more precises if lag times would be calculated per instrument & project
    non_terminal_mask = ~df["status"].isin(TERMINAL_STATUSES)
    eta_timestamps = (
        df.loc[non_terminal_mask, "created_at_"] + pd.Timedelta(seconds=lag_time)
    ).dt.tz_localize("UTC")

    # Convert ETA timestamps to human-readable format showing "in X time"
    def _format_eta(eta_time: datetime) -> str:
        """Format the eta time to a string."""
        time_diff = eta_time.replace(microsecond=0) - now
        if eta_time <= now:
            return f"now ({time_diff})"
        return get_display_time(now - time_diff, now, prefix="in ", suffix="")

    return eta_timestamps.apply(_format_eta)


def _get_yerror_column_name(y_column_name: str, df: pd.DataFrame) -> str | None:
    """Get the name of the error column for `y_column_name`, if it endwith '_mean' and is available in the `df`."""
    if not y_column_name.endswith("_mean"):
        return None

    if (yerror_column_name := y_column_name.replace("_mean", "_std")) not in df.columns:
        return None

    return yerror_column_name


def _calculate_trendline(
    x_data: pd.Series, y_data: pd.Series
) -> tuple[np.ndarray, np.ndarray] | None:
    """Calculate linear regression trendline for the given x and y data."""
    # Remove NaN values
    mask = ~(pd.isna(x_data) | pd.isna(y_data))
    x_clean = x_data[mask]
    y_clean = y_data[mask]

    if len(x_clean) < 2:  # noqa: PLR2004
        return None

    # Check if y_data is numeric, if not return None
    if not pd.api.types.is_numeric_dtype(y_clean):
        return None

    # Convert x data to numeric
    if pd.api.types.is_numeric_dtype(x_clean):
        x_numeric = x_clean
    elif pd.api.types.is_datetime64_any_dtype(x_clean):
        x_numeric = pd.to_numeric(x_clean)
    else:
        # Try to convert to datetime first, then to numeric
        try:
            x_datetime = pd.to_datetime(x_clean)
            x_numeric = pd.to_numeric(x_datetime)
        except (ValueError, TypeError):
            # If that fails, try direct numeric conversion
            try:
                x_numeric = pd.to_numeric(x_clean)
            except (ValueError, TypeError):
                return None

    # Perform linear regression
    coeffs = np.polyfit(x_numeric, y_clean, 1)

    # Generate trendline points
    x_trend = np.linspace(x_numeric.min(), x_numeric.max(), 100)
    y_trend = coeffs[0] * x_trend + coeffs[1]

    # Convert back to datetime if needed
    if pd.api.types.is_datetime64_any_dtype(x_clean) or (
        not pd.api.types.is_numeric_dtype(x_clean)
        and not pd.api.types.is_datetime64_any_dtype(x_clean)
    ):
        # Convert back to datetime for plotting
        x_trend = pd.to_datetime(x_trend)

    return x_trend, y_trend
