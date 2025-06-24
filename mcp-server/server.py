"""A MCP server for accessing data in the AlphaKraken database."""

import math

# ruff: noqa: T201  # `print` found
# ruff: noqa: BLE001  # Do not catch blind exception
# ruff: noqa: ANN401  #  Dynamically typed expressions (typing.Any) are disallowed
import sys
from datetime import datetime, timedelta
from typing import Any

import pytz
from mcp.server.fastmcp import FastMCP
from mongoengine import QuerySet

from shared.db.engine import connect_db
from shared.db.models import Metrics, RawFile

mcp = FastMCP("AlphaKraken")


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
    "status",
    "status_details",
    "size",
    "created_at",
]
metrics_keys_blacklist = ["_id", "raw_file", "created_at_"]
basic_metrics_keys = ["proteins", "raw:gradient_length_m"]

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
        connect_db()
        raw_files = RawFile.objects.filter(id__in=raw_file_names)

        results = augment_raw_files_with_metrics(raw_files, gradient_length_filter=False, only_basic_metrics=False)

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
        max_age_in_days (int): The maximum age of raw files in days. Default is 7.
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

    cutoff = datetime.now(tz=pytz.utc) - timedelta(days=max_age_in_days)
    try:
        connect_db()
        raw_files = RawFile.objects(
            instrument_id=instrument_id,
            id__icontains=name_search_string,
            created_at__gte=cutoff,
        )
        results = augment_raw_files_with_metrics(raw_files, gradient_length_filter=gradient_length_filter, only_basic_metrics=only_basic_metrics)

    except Exception as e:
        msg = f"Failed to retrieve raw file data: {e}"
        print(msg, file=sys.stderr)
        results = [{"error": msg}]

    return results


def augment_raw_files_with_metrics(raw_files: QuerySet, *, gradient_length_filter: float | None, only_basic_metrics : bool) ->  list[dict[str, Any]]                              :

    raw_files_dict: dict = {dict(raw_file.to_mongo())["_id"]: dict(raw_file.to_mongo()) for raw_file in raw_files}

    # querying all metrics at once to avoid load on DB
    for m in Metrics.objects.filter(raw_file__in=list(raw_files_dict.keys())).order_by("-created_at_"):
        mm = dict(m.to_mongo())
        raw_files_dict[mm["raw_file"]]["metrics"] = mm  # here we overwrite older metrics for a raw file if any

    results = []
    for raw_file in raw_files_dict.values():
        metrics = raw_file.get("metrics")

        if gradient_length_filter and (
                not metrics or not metrics["raw:gradient_length_m"] * 0.95 <= gradient_length_filter <= metrics[
            "raw:gradient_length_m"] * 1.05):
            continue

        # TODO this needs to be combined with the above step
        metrics_dict = (
            {
                k: _format(v)
                for k, v in metrics.items()
                if k not in metrics_keys_blacklist
                   and (k in basic_metrics_keys or not only_basic_metrics)
            }
            if metrics
            else {}
        )

        results.append(
            {

                # TODO this needs to be combined with the above step
                "raw_file": {
                                k: _format(v)
                                for k, v in raw_file.items()
                                if k in raw_file_keys_whitelist
                            }
                            | {"name": raw_file["_id"]},
                "metrics": metrics_dict,
            }
        )
    return results

#get_raw_files("astral2", max_age_in_days=30)  # only for debugging
mcp.run()
