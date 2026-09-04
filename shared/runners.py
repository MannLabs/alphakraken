"""Named runners: the configured places where quanting jobs execute."""

from dataclasses import dataclass
from pathlib import PurePath, PurePosixPath, PureWindowsPath
from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from shared.keys import ConstantsClass, JobEngines
from shared.path_views import Locations, View
from shared.yamlsettings import YAMLSETTINGS, YamlKeys


class OperatingSystems(metaclass=ConstantsClass):
    """Operating systems a runner can have, determining the path flavour of its view."""

    LINUX = "linux"
    MACOS = "macos"
    WINDOWS = "windows"


# PurePath flavours only: runner paths are rendered for another machine, never opened here
_OS_TO_PATH_CLASS: dict[str, type[PurePath]] = {
    OperatingSystems.LINUX: PurePosixPath,
    OperatingSystems.MACOS: PurePosixPath,
    OperatingSystems.WINDOWS: PureWindowsPath,
}


class _RunnerEntry(BaseModel):
    """One entry of the yaml `runners` list, the field names being the yaml keys."""

    model_config = ConfigDict(extra="forbid")

    name: str
    engine: str
    os: str
    view: dict[str, str]
    ssh_connection_id_prefix: str | None = None

    @field_validator("engine")
    @classmethod
    def _engine_is_known(cls, engine: str) -> str:
        if engine not in JobEngines.get_values():
            raise ValueError(
                f"unknown engine '{engine}', known are: {JobEngines.get_values()}"
            )
        return engine

    @field_validator("os")
    @classmethod
    def _os_is_known(cls, os: str) -> str:
        if os not in _OS_TO_PATH_CLASS:
            raise ValueError(f"unknown os '{os}', known are: {list(_OS_TO_PATH_CLASS)}")
        return os

    @field_validator("view")
    @classmethod
    def _view_keys_are_locations(cls, view: dict[str, str]) -> dict[str, str]:
        if unknown := set(view) - set(Locations.get_values()):
            raise ValueError(
                f"unknown view keys {sorted(unknown)}, known are: {sorted(Locations.get_values())}"
            )
        return view


@dataclass(frozen=True)
class Runner:
    """A configured place where jobs execute."""

    name: str
    engine: str
    os: str  # kept for the future SSH handler (job script per OS)
    view: View[PurePath]
    ssh_connection_id_prefix: str | None  # engines that need it check for None


def _label(entry: Any, index: int) -> str:
    """Identify a runner entry in error messages by its name, or by its position if it has none."""
    name = entry.get("name") if isinstance(entry, dict) else None
    return f"'{name}'" if name else f"#{index}"


def _build_runners(entries: list[dict[str, Any]] | None) -> dict[str, Runner]:
    """Validate the yaml `runners` list and build the runners keyed by name, order kept."""
    if not entries:
        return {}

    runners: dict[str, Runner] = {}
    for index, entry in enumerate(entries):
        try:
            parsed = _RunnerEntry.model_validate(entry)
        except ValidationError as e:
            raise ValueError(
                f"Runner {_label(entry, index)} in alphakraken.yaml: {e}"
            ) from e

        if parsed.name in runners:
            raise ValueError(
                f"Runner '{parsed.name}': duplicate name in alphakraken.yaml."
            )

        runners[parsed.name] = Runner(
            name=parsed.name,
            engine=parsed.engine,
            os=parsed.os,
            view=View(parsed.name, parsed.view, _OS_TO_PATH_CLASS[parsed.os]),
            ssh_connection_id_prefix=parsed.ssh_connection_id_prefix,
        )

    return runners


RUNNERS: dict[str, Runner] = _build_runners(YAMLSETTINGS.get(YamlKeys.RUNNERS))


def get_runner(name: str) -> Runner:
    """Get the runner declared under `name` in alphakraken.yaml."""
    if name not in RUNNERS:
        raise KeyError(
            f"Unknown runner '{name}', declared in alphakraken.yaml are: {list(RUNNERS)}."
        )
    return RUNNERS[name]
