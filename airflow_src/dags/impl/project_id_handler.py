"""Module to extract the project ID from the raw file name.

This requires a lot of heuristics at the moment, as we expect the project id anywhere after the "_SA_" token.
Because the naming convention is so loose, we need to compare each part of the file name with the actual projects
in the DB.
Could be simplified by a more strict naming convention, e.g. "2021-01-01_something__<project_id>_something.d".
"""

import logging
import re


def _get_after_token(
    string_to_parse: str,
    initial_token: str,
    sep: str = "_",
) -> list[str]:
    """Extracts everything after the first occurrence of an (optional) token as a list of strings (separated by `sep`).

    :param string_to_parse: input string to search for the token
    :param initial_token: everything after the first occurrence of this token will be extracted.
        If empty, the whole string will be extracted.
    :param sep: separator to split the extracted part
    :return: list of strings, empty if not exactly one match of the token was found
    """
    if initial_token == "":
        return string_to_parse.split(sep)

    if string_to_parse.count(initial_token) > 1:
        logging.warning(f"Input string contains more than one '{initial_token}'")
        return []
    match = re.search(f"{initial_token}(.*)", string_to_parse)
    return match.group(1).split(sep) if match else []


def _get_unique_overlap(list1: list[str], list2: list[str]) -> str | None:
    """Compare two lists and return the intersection if it is exactly one element.

    :param list1: first list
    :param list2: second list
    :return: the unique element if the intersection is exactly one element, otherwise None
    """
    intersection = set(list1).intersection(list2)
    if len(intersection) == 1:
        return list(intersection)[0]  # noqa: RUF015
    if len(intersection) > 1:
        logging.warning("found more than 1 match")
        return None
    return None


def get_unique_project_id(
    raw_file_name: str,
    project_ids: list[str],
    initial_token: str = "_SA_",  # noqa: S107 # Possible hardcoded password
) -> str | None:
    """Extract the project ID from the raw file name and return it if it is unique."""
    tokens = _get_after_token(raw_file_name, initial_token)
    return _get_unique_overlap(project_ids, tokens)
