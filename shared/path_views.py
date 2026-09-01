"""Views on the data directories: the same tree, seen from different machines."""

from pathlib import Path, PurePath, PurePosixPath
from typing import Generic, TypeVar

from shared.keys import ConstantsClass, InternalPaths
from shared.yamlsettings import YAMLSETTINGS, YamlKeys


class Locations(metaclass=ConstantsClass):
    """The data directories that can be addressed within a view.

    The values are the keys of the `locations` section of the yaml settings, and at the same time
    the folder names below the mounts folder within the containers.
    """

    INSTRUMENTS = "instruments"
    BACKUP = "backup"
    OUTPUT = "output"
    SETTINGS = "settings"
    SOFTWARE = "software"
    SLURM = "slurm"
    LOGS = "logs"


# the path flavor a view is expressed in, `Path` for the machine this code runs on
_P = TypeVar("_P", bound=PurePath)

# the locations that are mounted into the containers, cf. docker-compose.yaml
_MOUNTED_LOCATIONS = (Locations.INSTRUMENTS, Locations.BACKUP, Locations.OUTPUT)


class View(Generic[_P]):
    """The absolute paths of the data directories as seen from one machine (container, cluster, ..).

    Not every location is reachable from every machine.
    """

    def __init__(
        self, name: str, locations: dict[str, str], path_class: type[_P]
    ) -> None:
        """Initialize the View.

        :param name: name of the view, used in error messages
        :param locations: absolute path of each reachable location, cf. `Locations`
        :param path_class: `Path` for the machine this code runs on, a `PurePath` flavor otherwise
        """
        self._name = name
        self._locations = {
            location: path_class(path) for location, path in locations.items()
        }

    def has(self, location: str) -> bool:
        """Whether the given `location` is reachable in this view."""
        return location in self._locations

    def resolve(self, location: str, rel_path: PurePath | str = "") -> _P:
        """Get the absolute path of `rel_path`, which is relative to `location`, in this view."""
        if location not in self._locations:
            raise KeyError(
                f"Location '{location}' is not reachable in the '{self._name}' view, "
                f"reachable are: {sorted(self._locations)}."
            )
        return self._locations[location] / rel_path


AIRFLOW_CONTAINER_VIEW: View[Path] = View(
    "airflow container",
    {
        location: f"{InternalPaths.MOUNTS_PATH}{location}"
        for location in _MOUNTED_LOCATIONS
    },
    Path,
)


def _build_cluster_view() -> View[PurePosixPath]:
    """Build the view of a machine that accesses the data via the shared file system."""
    locations: dict[str, dict[str, str]] = YAMLSETTINGS.get(YamlKeys.LOCATIONS, {})  # type: ignore[invalid-assignment]

    absolute_paths = {
        location: values[YamlKeys.ABSOLUTE_PATH]
        for location, values in locations.items()
        if YamlKeys.ABSOLUTE_PATH in values
    }

    return View("cluster", absolute_paths, PurePosixPath)


def _build_docker_host_view() -> View[PurePosixPath]:
    """Build the view from within the processing (e.g. msqc) docker containers.

    Note this is not the airflow containers, they use InternalPaths.
    The docker daemon resolves bind mounts in host coordinates, so the paths handed to it need
    to be translated from the container view to this one.

    `locations.general.mounts_path` is required by the `docker` job engine only, so a missing key
    yields a view without locations rather than an error: it is reported when the view is used.
    """
    mounts_path = (
        YAMLSETTINGS.get(YamlKeys.LOCATIONS, {})  # type: ignore[possibly-unbound-attribute]
        .get(YamlKeys.Locations.GENERAL, {})
        .get(YamlKeys.Locations.MOUNTS_PATH)
    )

    locations = (
        {}
        if mounts_path is None
        else {location: f"{mounts_path}/{location}" for location in _MOUNTED_LOCATIONS}
    )

    return View("docker host", locations, PurePosixPath)


CLUSTER_VIEW: View[PurePosixPath] = _build_cluster_view()
DOCKER_HOST_VIEW: View[PurePosixPath] = _build_docker_host_view()
