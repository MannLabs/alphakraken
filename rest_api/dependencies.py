"""Query parameters shared by the routes of the REST API."""

from dataclasses import dataclass
from typing import Annotated

from fastapi import Query

MAX_LIMIT = 1000
DEFAULT_LIMIT = 100


@dataclass(frozen=True)
class Pagination:
    """The page of a list to return."""

    limit: Annotated[
        int, Query(ge=1, le=MAX_LIMIT, description="Number of results per page")
    ] = DEFAULT_LIMIT
    offset: Annotated[int, Query(ge=0, description="Number of results to skip")] = 0
