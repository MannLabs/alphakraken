"""Column configuration for the web application."""

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class Column:
    """Data class for information on how to display a data column (or multiple data columns if wildcards are used)."""

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
    # draw all matched wildcard columns as overlaid traces in one plot
    overlay: bool = False
    # not taken from yaml but set after parsing:
    # columns matched by a wildcard pattern, set by expand_columns
    matched_columns: list[str] | None = None


COLUMNS_CONFIG_FILE_PATH = Path(__file__).parent / ".." / "columns_config.yaml"


def load_columns_from_yaml() -> tuple[Column, ...]:
    """Load column configuration from YAML file."""
    with COLUMNS_CONFIG_FILE_PATH.open() as f:
        columns_config = yaml.safe_load(f)

    return tuple(
        Column(
            name=column["name"],
            hide=column.get("hide"),
            at_end=column.get("at_end"),
            color_gradient=column.get("color_gradient"),
            plot=column.get("plot"),
            log_scale=column.get("log_scale"),
            alternative_names=column.get("alternative_names"),
            plot_optional=column.get("plot_optional"),
            overlay=column.get("overlay", False),
        )
        for column in columns_config["columns"]
    )


def build_alternative_names_mapping(
    columns: tuple[Column, ...],
) -> dict[str, str]:
    """Build a mapping from alternative DB column names to their canonical names."""
    return {
        alt_name: column.name
        for column in columns
        if column.alternative_names
        for alt_name in column.alternative_names
    }
