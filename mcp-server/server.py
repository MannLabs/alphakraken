"""A MCP server for accessing data in the AlphaKraken database."""
# ruff: noqa: T201  # `print` found
# ruff: noqa: BLE001  # Do not catch blind exception

import sys
from typing import Any

try:
    from mcp.server.fastmcp import FastMCP

    from shared.db.engine import connect_db
    from shared.db.interface import get_raw_files_by_age
except Exception as e:
    print(f"Failed to connect to the database: {e}", file=sys.stderr)

mcp = FastMCP("AlphaKraken")


@mcp.tool()
def get_raw_files(
    instrument_id: str, max_age_in_days: int = 30, min_age_in_days: int = 0
) -> list[dict[str, Any]]:
    """Fetch raw files from the database for a given instrument within a specified age range."""
    print("enter get_raw_files", file=sys.stderr)
    try:
        connect_db()
    except Exception as e:
        print(f"Failed connect_db(): {e}", file=sys.stderr)

    try:
        raw_files_by_age = get_raw_files_by_age(
            instrument_id,
            max_age_in_days=max_age_in_days,
            min_age_in_days=min_age_in_days,
        )
    except Exception as e:
        print(f"Failed get_raw_files_by_age: {e}", file=sys.stderr)

    return [raw_file.to_mongo() for raw_file in raw_files_by_age]


mcp.run()
