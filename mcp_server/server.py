"""A MCP server for accessing data in the AlphaKraken database."""

import logging
import math
import os

# ruff: noqa: T201  # `print` found
# ruff: noqa: BLE001  # Do not catch blind exception
# ruff: noqa: ANN401  #  Dynamically typed expressions (typing.Any) are disallowed
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

from mcp.server.fastmcp import FastMCP

from shared.db import metrics, raw_files, status
from shared.domain.metrics import Metrics
from shared.domain.raw_files import RawFile

mcp = FastMCP(
    name="AlphaKraken",
    instructions="Get information about acquired files on mass spectrometer instruments.",
)
logger = logging.getLogger(__name__)


def _format(x: Any, n_digits: int = 5) -> Any:
    """Reduce information content of numbers and date fields for LLMs.

    Round a number to a specified number of digits, handling edge cases.
    Format datetime objects to a string representation.
    Leave strings and other types unchanged.
    """
    if isinstance(x, str):
        return x
    if isinstance(x, datetime):
        return x.replace(microsecond=0).replace(second=0).isoformat()
    if not isinstance(x, (int, float)):
        return x

    if x == 0:
        return 0
    try:
        ret_val = round(x, n_digits - int(math.floor(math.log10(abs(x)))) - 1)
    except Exception:
        ret_val = x
    return int(ret_val) if isinstance(ret_val, int) else ret_val


# some thoughts for future improvements:
# TODO: offer combined queries, like multiple instruments etc
# TODO: offer a dict of metrics with its description, plus a filter to retrieve only certain metrics
# TODO: offer regexps
# TODO: think about pagination, if the number of files is large
# TODO: make this a rest API?
# TODO: add raw file location -> to allow other tools to pick up the raw files


raw_file_keys_whitelist = [
    "instrument_id",
    "status",
    "status_details",
    "size",
    "created_at",
]
basic_metrics_keys = ["proteins", "gradient_length"]
GRADIENT_LENGTH_KEY = "gradient_length"
# relative tolerance of the gradient length filter
GRADIENT_LENGTH_TOLERANCE = 0.05


@mcp.tool()
def get_available_instruments() -> list[dict[str, Any]]:
    """Retrieve a list of all available instruments.

    Returns:
        list[dict[str, Any]]: A list of dictionaries containing instrument information.
                             Each dictionary has keys:
                             - "instrument_id": The ID of the instrument
                             If an error occurs, returns a list with one dictionary containing an "error" key.

    """
    try:
        results = [
            {"instrument_id": instrument_id}
            for instrument_id in status.instrument_ids()
        ]

    except Exception as e:
        msg = f"Failed to retrieve instrument data: {e}"
        print(msg, file=sys.stderr)
        results = [{"error": msg}]

    return results


@mcp.tool()
def get_raw_files_by_names(
    raw_file_names: list[str],
) -> list[dict[str, Any]]:
    """Retrieve raw files by their names and their latest metrics from the database.

    Args:
        raw_file_names (list[str]): A list of raw file names to search for in the database. Case sensitive, needs to match exactly.

    Returns:
        list[dict[str, Any]]: A list of dictionaries containing raw file information and their latest metrics.
                              Each dictionary has the keys:
                              - "raw_file": A dictionary with raw file details.
                              - "metrics": A dictionary with the latest metrics or an empty dictionary if none exist.
                              If an error occurs, the list contains one dictionary with an "error" key.

    """
    try:
        found = raw_files.find(raw_files.RawFileFilter(ids=raw_file_names))
        results = _with_latest_metrics(
            found, gradient_length_filter=None, only_basic_metrics=False
        )

    except Exception as e:
        msg = f"Failed to retrieve raw file data: {e}"
        print(msg, file=sys.stderr)
        results = [{"error": msg}]

    return results


@mcp.tool()
def get_raw_files_for_instrument(
    instrument_id: str,
    name_search_string: str = "",
    max_age_in_days: int = 30,
    gradient_length_filter: float | None = None,
    *,
    only_basic_metrics: bool = True,
) -> list[dict[str, Any]]:
    """Retrieve raw files for a specific instrument and their latest metrics from the database.

    Args:
        instrument_id (str): The ID of the instrument to filter raw files.
        name_search_string (str): A substring to search for in raw file IDs, case insensitive. Default is an empty string.
        max_age_in_days (int): The maximum age of raw files in days. Default is 30.
        gradient_length_filter (float | None): If not None, filters raw_files by gradient length (minutes).
            Raw files without metrics or outside this range are excluded. Filter has a tolerance of 5% around the provided value.
            Default is None.
        only_basic_metrics (bool): If True, only basic metrics (gradient_length, number of proteins) are returned, for a quick overview.
            Default is True.

    Returns:
        list[dict[str, Any]]: A list of dictionaries containing raw file information and their latest metrics.
                              Each dictionary has the keys:
                              - "raw_file": A dictionary with raw file details.
                              - "metrics": A dictionary with the latest metrics or an empty dictionary if none exist.
                              If an error occurs, the list contains one dictionary with an "error" key.

    """
    # Note: the docstring is used by the MCP server to generate the API documentation for the LLM!

    cutoff = datetime.now(tz=UTC) - timedelta(days=max_age_in_days)
    try:
        found = raw_files.find(
            raw_files.RawFileFilter(
                instrument_id=instrument_id,
                name_contains=name_search_string or None,
                created_after=cutoff,
            )
        )
        results = _with_latest_metrics(
            found,
            gradient_length_filter=gradient_length_filter,
            only_basic_metrics=only_basic_metrics,
        )

    except Exception as e:
        msg = f"Failed to retrieve raw file data: {e}"
        print(msg, file=sys.stderr)
        results = [{"error": msg}]

    return results


def _flatten_metrics(by_type: dict[str, Metrics]) -> dict[str, Any]:
    """Flatten metrics from different types into a single dict with type prefixes for conflicts.

    On key collisions, only the new key will carry a prefix.
    The assumption is that collisions are rare and only happen for very generic keys like "proteins".
    """
    flattened: dict[str, Any] = {}
    for metrics_type, type_metrics in by_type.items():
        for key, value in {
            **_metadata(type_metrics),
            **type_metrics.values,
        }.items():
            if key not in flattened:
                flattened[key] = value
            else:
                flattened[f"{metrics_type}_{key}"] = value

    return flattened


def _metadata(type_metrics: Metrics) -> dict[str, Any]:
    """Return the metadata of the metrics that is exposed next to the values, skipping unset ones."""
    metadata = {
        "settings_name": type_metrics.settings_name,
        "settings_version": type_metrics.settings_version,
        "relative_output_path": type_metrics.relative_output_path,
        "output_display_base_path": type_metrics.output_display_base_path,
    }
    return {k: v for k, v in metadata.items() if v is not None}


def _matches_gradient_length(gradient_length: Any, target: float) -> bool:
    """Return whether the gradient length is within the tolerance of the target; a missing one never is."""
    if gradient_length is None:
        return False
    return (
        float(gradient_length) * (1 - GRADIENT_LENGTH_TOLERANCE)
        <= target
        <= float(gradient_length) * (1 + GRADIENT_LENGTH_TOLERANCE)
    )


def _with_latest_metrics(
    found: list[RawFile],
    *,
    gradient_length_filter: float | None,
    only_basic_metrics: bool,
) -> list[dict[str, Any]]:
    """Combine raw files with their latest metrics.

    Args:
        found (list[RawFile]): The raw files to combine with their metrics.
        gradient_length_filter (float | None): If not None, filters raw_files by gradient length (minutes).
            Raw files without metrics or outside this range are excluded. Filter has a tolerance of +/- 5% around the provided value.
        only_basic_metrics (bool): If True, only basic metrics (gradient_length, number of proteins) are returned, for a quick overview.

    Returns:
        list[dict[str, Any]]: A list of dictionaries containing raw file information and their latest metrics.
                              Each dictionary has the keys:
                              - "raw_file": A dictionary with raw file details.
                              - "metrics": A dictionary with the latest metrics or an empty dictionary if none exist.

    """
    latest_metrics = metrics.latest_by_raw_file([raw_file.id for raw_file in found])

    results = []
    for raw_file in found:
        flattened = _flatten_metrics(latest_metrics.get(raw_file.id, {}))

        if gradient_length_filter and not _matches_gradient_length(
            flattened.get(GRADIENT_LENGTH_KEY), gradient_length_filter
        ):
            continue

        results.append(
            {
                "raw_file": {
                    k: _format(v)
                    for k, v in vars(raw_file).items()
                    if k in raw_file_keys_whitelist
                }
                | {"name": raw_file.id},
                "metrics": {
                    k: _format(v)
                    for k, v in flattened.items()
                    if k in basic_metrics_keys or not only_basic_metrics
                },
            }
        )
    return results


mcp_transport = os.getenv("MCP_TRANSPORT", "stdio")

if mcp_transport == "stdio":
    mcp.run()
elif mcp_transport == "streamable-http":
    # https://github.com/jlowin/fastmcp/issues/873#issuecomment-2997928922
    # Intentionally binding to all interfaces for container deployment
    mcp.settings.host = "0.0.0.0"  # noqa: S104
    mcp.settings.port = int(os.getenv("MCP_PORT", None))
    # Stateless mode: no per-backend session state, so requests can be load-balanced
    # across hosts and survive server restarts without a "Missing session ID" 400.
    mcp.settings.stateless_http = True
    mcp.run(transport="streamable-http")
elif mcp_transport == "disabled":
    logging.info("MCP server disabled, not starting.")
else:
    raise ValueError(
        "Please select a supported MCP transport type: stdio,streamable-http,disabled"
    )
