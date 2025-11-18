"""Plotting and data-related functionality for the overview page."""

import numpy as np
import pandas as pd
import plotly.express as px
from pages_.impl.overview_utils import (
    Column,
    filter_valid_columns,
)
from service.utils import Cols, display_plotly_chart

from shared.db.models import ERROR_STATUSES


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

    return x_trend, y_trend  # type: ignore[invalid-return-type]


def _draw_plot(  # noqa: PLR0913
    df_with_baseline: pd.DataFrame,
    *,
    x: str,
    column: Column,
    color_by_column: str,
    show_traces: bool,
    show_std: bool,
    show_trendline: bool,
) -> None:
    """Draw a plot of a DataFrame."""
    df_with_baseline = df_with_baseline.sort_values(by=x)

    df = df_with_baseline[~df_with_baseline[Cols.IS_BASELINE]]
    y = column.name
    y_is_numeric = pd.api.types.is_numeric_dtype(df[y])
    median_ = df[y].median() if y_is_numeric else 0
    title = f"{y} (median= {median_:.2f})" if y_is_numeric else y

    hover_data = filter_valid_columns(
        [
            "file_created",
            "size_gb",
            "precursors",
            "status",
        ],
        df,
    )

    if y == "status" and "status_details" in df:
        df.loc[pd.isna(df["status_details"]), "status_details"] = ""
        hover_data.append("status_details")

    fig = px.scatter(
        df,
        x=x,
        y=y,
        color=color_by_column,
        hover_name="_id",
        hover_data=hover_data,
        title=title,
        height=400,
        error_y=_get_yerror_column_name(y, df) if show_std else None,
        log_y=column.log_scale,
    )
    if y_is_numeric and show_traces:
        symbol = [
            "x" if x in ERROR_STATUSES else "circle" for x in df["status"].to_numpy()
        ]
        fig.update_traces(mode="lines+markers", marker={"symbol": symbol})
    fig.add_hline(y=median_, line_dash="dash", line={"color": "lightgrey"})

    # Add baseline if baseline data is available
    if y_is_numeric:
        baseline_df = df_with_baseline[df_with_baseline[Cols.IS_BASELINE]]
        if len(baseline_df) > 0 and y in baseline_df.columns:
            baseline_mean = baseline_df[y].mean()
            baseline_std = baseline_df[y].std()

            fig.add_hline(
                y=baseline_mean,
                line_dash="dash",
                line={"color": "green"},
                annotation_text=f"Baseline Mean: {baseline_mean:.2f} ± {baseline_std:.2f}",
                annotation_position="bottom right",
            )
            if not pd.isna(baseline_std):
                for err in [-baseline_std, baseline_std]:
                    fig.add_hline(
                        y=baseline_mean + err,
                        line_dash="dot",
                        line={"color": "green"},
                    )

    # Add trendline if requested and data is numeric
    if show_trendline and y_is_numeric:
        trendline_data = _calculate_trendline(df[x], df[y])
        if trendline_data is not None:
            x_trend, y_trend = trendline_data
            fig.add_scatter(
                x=x_trend,
                y=y_trend,
                mode="lines",
                name="Trendline",
                line={"color": "black"},
                showlegend=True,
            )

    display_plotly_chart(fig)
