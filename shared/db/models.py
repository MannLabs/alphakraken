"""Module containing the base database model class and all models."""

from __future__ import annotations

import abc
import dataclasses
import inspect
from dataclasses import dataclass
from typing import Any, ClassVar


@dataclass
class MongoBaseModel(abc.ABC):
    """Base class for all models."""

    # the name of the table
    # 'ClassVar' type annotation means that it will not be filled on object instantiation
    table: ClassVar[str] = None

    @property
    def pk(self) -> dict[str, str]:
        """The primary key of the model. Must be implemented in subclasses.

        This key will be used to query models for updates. Must be unique.
        """
        raise ValueError("pk property must be implemented in subclasses")

    def to_dict(self) -> dict[str, Any]:
        """Convert the dataclass to a dictionary with non-None values."""
        # https://stackoverflow.com/a/62730782
        return {
            key: value
            for key, value in dataclasses.asdict(self).items()
            if value is not None
        }

    @classmethod
    def from_dict(cls, env: dict) -> MongoBaseModel:
        """Create a new instance of the dataclass from a dictionary ignoring all fields that are not part of it."""
        # https://stackoverflow.com/a/55096964
        return cls(
            **{k: v for k, v in env.items() if k in inspect.signature(cls).parameters}
        )


@dataclass
class RawFile(MongoBaseModel):
    """Model for raw files."""

    table: ClassVar[str] = "raw_files"

    name: str
    status: str = None

    @property
    def pk(self) -> dict[str, str]:
        """Primary key of the model."""
        return {"name": self.name}
