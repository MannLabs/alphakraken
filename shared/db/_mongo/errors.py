"""Translation of mongoengine and pymongo exceptions into domain errors."""

import functools
from collections.abc import Callable
from typing import ParamSpec, TypeVar

import mongoengine
import pymongo.errors
from mongoengine.errors import MongoEngineException

from shared.domain.errors import (
    AlreadyExistsError,
    DatabaseError,
    DatabaseUnavailableError,
    NotFoundError,
    ValidationError,
)

P = ParamSpec("P")
R = TypeVar("R")


def translate_errors(func: Callable[P, R]) -> Callable[P, R]:
    """Raise domain errors instead of mongoengine and pymongo ones."""

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return func(*args, **kwargs)
        except mongoengine.DoesNotExist as e:
            raise NotFoundError(str(e)) from e
        except (mongoengine.NotUniqueError, pymongo.errors.DuplicateKeyError) as e:
            raise AlreadyExistsError(str(e)) from e
        except mongoengine.ValidationError as e:
            raise ValidationError(_messages(e)) from e
        except pymongo.errors.ConnectionFailure as e:
            raise DatabaseUnavailableError(str(e)) from e
        except (
            pymongo.errors.PyMongoError,
            mongoengine.ConnectionFailure,
            # on writes mongoengine wraps pymongo's OperationFailure in its own OperationError
            MongoEngineException,
        ) as e:
            raise DatabaseError(str(e)) from e

    return wrapper


def _messages(error: mongoengine.ValidationError) -> list[str]:
    """Return one message per invalid field, or the error's own message if it names none."""
    errors = error.to_dict()
    if not errors:
        return [str(error)]
    return [f"{field}: {message}" for field, message in errors.items()]
