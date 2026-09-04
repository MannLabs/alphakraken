"""Named runners: the configured places where quanting jobs execute."""

from dataclasses import dataclass
from pathlib import PurePath, PurePosixPath, PureWindowsPath
from typing import Any

from shared.keys import ConstantsClass, JobEngines
from shared.path_views import Locations, View
from shared.yamlsettings import YAMLSETTINGS, YamlKeys


class OperatingSystems(metaclass=ConstantsClass):
    """Operating systems a runner can have, determining the path flavour of its view."""

    LINUX = "linux"
    MACOS = "macos"
    WINDOWS = "windows"


# never `Path`: no code does filesystem I/O in a runner view
_OS_TO_PATH_CLASS: dict[str, type[PurePath]] = {
    OperatingSystems.LINUX: PurePosixPath,
    OperatingSystems.MACOS: PurePosixPath,
    OperatingSystems.WINDOWS: PureWindowsPath,
}


@dataclass(frozen=True)
class Runner:
    """A configured place where jobs execute."""

    name: str
    engine: str
    os: str
    view: View[PurePath]
    ssh_connection_id_prefix: str | None


def _get_value(entry: dict[str, Any], key: str, runner_name: str) -> Any:
    """Get `key` from a runner entry, raising if it is missing."""
    if key not in entry:
        raise KeyError(
            f"Runner '{runner_name}': key `{key}` is missing in alphakraken.yaml."
        )
    return entry[key]


def _build_runner(entry: dict[str, Any]) -> Runner:
    """Validate one yaml runner entry and build the runner."""
    name = entry.get(YamlKeys.Runners.NAME)
    if name is None:
        raise KeyError(
            f"A runner is missing its `{YamlKeys.Runners.NAME}` in alphakraken.yaml: {entry}"
        )

    engine = _get_value(entry, YamlKeys.Runners.ENGINE, name)
    if engine not in JobEngines.get_values():
        raise ValueError(
            f"Runner '{name}': unknown `{YamlKeys.Runners.ENGINE}` '{engine}', "
            f"known are: {JobEngines.get_values()}."
        )

    os = _get_value(entry, YamlKeys.Runners.OS, name)
    if os not in _OS_TO_PATH_CLASS:
        raise ValueError(
            f"Runner '{name}': unknown `{YamlKeys.Runners.OS}` '{os}', "
            f"known are: {list(_OS_TO_PATH_CLASS)}."
        )

    view = _get_value(entry, YamlKeys.Runners.VIEW, name)
    if unknown := set(view) - set(Locations.get_values()):
        raise ValueError(
            f"Runner '{name}': unknown `{YamlKeys.Runners.VIEW}` keys {sorted(unknown)}, "
            f"known are: {sorted(Locations.get_values())}."
        )

    return Runner(
        name=name,
        engine=engine,
        os=os,
        view=View(name, view, _OS_TO_PATH_CLASS[os]),
        ssh_connection_id_prefix=entry.get(YamlKeys.Runners.SSH_CONNECTION_ID_PREFIX),
    )


def _build_runners(entries: list[dict[str, Any]] | None) -> dict[str, Runner]:
    """Validate the yaml `runners` list and build the runners keyed by name, order kept."""
    if not entries:
        raise ValueError(
            f"`{YamlKeys.RUNNERS}` in alphakraken.yaml must be a non-empty list."
        )

    runners: dict[str, Runner] = {}
    for entry in entries:
        runner = _build_runner(entry)
        if runner.name in runners:
            raise ValueError(
                f"Runner '{runner.name}': duplicate `{YamlKeys.Runners.NAME}` in alphakraken.yaml."
            )
        runners[runner.name] = runner

    return runners


RUNNERS: dict[str, Runner] = _build_runners(YAMLSETTINGS.get(YamlKeys.RUNNERS))


def get_runner(name: str) -> Runner:
    """Get the runner declared under `name` in alphakraken.yaml."""
    if name not in RUNNERS:
        raise KeyError(
            f"Unknown runner '{name}', declared in alphakraken.yaml are: {list(RUNNERS)}."
        )
    return RUNNERS[name]
