"""Building of the input types that user-triggered writes are validated with."""

from typing import Any, TypeVar

import pydantic

from shared.domain.errors import ValidationError

T = TypeVar("T", bound=pydantic.BaseModel)

# prepended by pydantic to the message of a ValueError raised in a validator
_VALUE_ERROR_PREFIX = "Value error, "


def parse(model: type[T], **fields: Any) -> T:
    """Build the input type from the given fields.

    Raises:
        ValidationError: if the fields are invalid, with one `<field>: <message>` entry per
            field and the bare message for a rule spanning several fields.

    """
    try:
        return model(**fields)
    except pydantic.ValidationError as e:
        raise ValidationError([_message(error) for error in e.errors()]) from e


def _message(error: Any) -> str:
    message = error["msg"].removeprefix(_VALUE_ERROR_PREFIX)
    field = ".".join(str(part) for part in error["loc"])
    return f"{field}: {message}" if field else message


def empty_to_none(value: Any) -> Any:
    """Return None for an empty or blank string, any other value as it is."""
    return None if isinstance(value, str) and not value.strip() else value


def max_length(model: type[pydantic.BaseModel], field: str) -> int:
    """Return the maximum length declared for a string field of the input type."""
    return next(
        constraint.max_length
        for constraint in model.model_fields[field].metadata
        if getattr(constraint, "max_length", None) is not None
    )
