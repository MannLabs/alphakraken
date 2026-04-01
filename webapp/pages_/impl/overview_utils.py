"""Utility functions for the overview page with no Streamlit dependencies."""

from datetime import datetime
from fnmatch import fnmatch

import pandas as pd
from service.columns import Column
from service.components import get_display_time
from service.data_handling import get_combined_raw_files_and_metrics_df
from service.query_params import QueryParams
from service.utils import APP_URL, BASELINE_PREFIX, FILTER_MAPPING, Cols

from shared.db.models import TERMINAL_STATUSES

EXPLANATION_STATUS = """
            #### Explanation of 'status' information
            - `done`: The file has been fully processed successfully.
            - `done_not_quanted`: The file has been handled successfully, but was not quanted (check the "status_details" column).
            - `acquisition_failed`: the acquisition of the file failed (check the "status_details" column).
            - `quanting_failed`: something went wrong with the quanting, check the "status_details" column for more information:
              - `NO_RECALIBRATION_TARGET`: alphaDIA did not find enough precursors to calibrate the data.
              - `NOT_DIA_DATA`: the file is not DIA data.
              - `TIMEOUT`: the quanting job took too long and was stopped
              - `_*`: a underscore as prefix indicates a known error, whose root cause has not been investigated yet.
              - `__*`: a double underscore as prefix indicates that there was an error while investigating the error.
            - `error`: an unknown error happened during processing, check the "status_details" column for more information
                and report it to the developers if unsure.

            All other states are transient and should be self-explanatory. If you feel a file stays in a certain status
            for too long, please report it to the developers.
        """


def expand_columns(
    columns: tuple[Column, ...], df_columns: list[str]
) -> tuple[Column, ...]:
    """Expand wildcard column entries against actual DataFrame columns.

    If overlay=False, each match becomes its own plotable Column.
    If overlay=True, individual matches get plot=False and one Column collecting all matching columns (for joint plotting) is added instead.

    """
    expanded = []
    for column in columns:
        # regular case: column is taking as is
        if "*" not in column.name:
            expanded.append(column)
            continue

        # 'wildcard' case
        matches = sorted(c for c in df_columns if fnmatch(c, column.name))
        if not matches:
            continue

        # all every column (to show up in the table), but don't plot individual columns if overlay=True
        for df_col in matches:
            plot = False if column.overlay else column.plot
            expanded.append(
                Column(
                    name=df_col,
                    hide=column.hide,
                    at_end=column.at_end,
                    color_gradient=column.color_gradient,
                    plot=plot,
                    log_scale=column.log_scale,
                    alternative_names=column.alternative_names,
                    plot_optional=column.plot_optional,
                )
            )
        if column.overlay and column.plot:
            expanded.append(
                Column(
                    name=column.name,
                    plot=True,
                    log_scale=column.log_scale,
                    plot_optional=column.plot_optional,
                    overlay=True,
                    matched_columns=matches,
                )
            )
    return tuple(expanded)


def get_column_order(df: pd.DataFrame, columns: tuple[Column, ...]) -> list[str]:
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


def filter_valid_columns(columns: list[str], df: pd.DataFrame) -> list[str]:
    """Filter out `columns` that are not in the `df`."""
    return [col for col in columns if col in df.columns]


def get_baseline_df(
    baseline_query_param: str,
) -> tuple[pd.DataFrame, int]:
    """Get the baseline DataFrame and the number of desired files based on the query parameter."""
    baseline_file_names = [name.strip() for name in baseline_query_param.split(",")]
    baseline_df, _ = get_combined_raw_files_and_metrics_df(
        raw_file_ids=baseline_file_names
    )
    if len(baseline_df) == 0:
        return baseline_df, len(baseline_file_names)

    baseline_df[Cols.IS_BASELINE] = True

    # this is a hack to prevent index clashing, but also helps to identify the baseline data in the table
    baseline_df.index = [BASELINE_PREFIX + str(idx) for idx in baseline_df.index]

    return baseline_df, len(baseline_file_names)


def get_url_with_query_string(user_input: str | None, query_params: dict) -> str:
    """Return the URL with the query string based on the user input."""
    encoded_user_input = user_input or ""
    for key, value in FILTER_MAPPING.items():
        encoded_user_input = encoded_user_input.replace(" ", "").replace(
            value.strip(), key
        )

    url = f"{APP_URL}/overview?{QueryParams.FILTER}={encoded_user_input}"

    for param in [
        QueryParams.MOBILE,
        QueryParams.MAX_TABLE_LEN,
        QueryParams.MAX_AGE,
        QueryParams.INSTRUMENTS,
        QueryParams.BASELINE,
    ]:
        if param in query_params:
            url += f"&{param}={query_params[param]}"

    return url.replace(" ", "")


def add_eta(df: pd.DataFrame, now: datetime, lag_time: float) -> pd.Series:
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
