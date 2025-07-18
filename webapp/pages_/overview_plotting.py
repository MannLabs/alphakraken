"""Plotting functionality for the overview page."""

import pandas as pd
import plotly.express as px
from pages_.overview_utils import (
    Column,
    _calculate_trendline,
    _filter_valid_columns,
    _get_yerror_column_name,
)
from service.utils import Cols, display_plotly_chart

from shared.db.models import ERROR_STATUSES


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

    hover_data = _filter_valid_columns(
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
