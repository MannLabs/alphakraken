"""Building of the input types that user-triggered writes are validated with."""

import re
from collections.abc import Callable
from typing import Annotated, Any, TypeVar

import pydantic

from shared.domain.errors import InvalidInputError


class InputModel(pydantic.BaseModel):
    """Base of the input types: immutable, strings stripped, unknown fields rejected."""

    model_config = pydantic.ConfigDict(
        frozen=True, str_strip_whitespace=True, extra="forbid"
    )


T = TypeVar("T", bound=InputModel)

# prepended by pydantic to the message of a ValueError raised in a validator
_VALUE_ERROR_PREFIX = "Value error, "


def parse(model: type[T], **fields: Any) -> T:
    """Build the input type from the given fields.

    Raises:
        InvalidInputError: if the fields are invalid, with one `<field>: <message>` entry per
            field and the bare message for a rule spanning several fields.

    """
    try:
        return model(**fields)
    except pydantic.ValidationError as e:
        raise InvalidInputError([message(error) for error in e.errors()]) from e


def message(error: Any) -> str:
    """Return the message of a pydantic error, prefixed by its field if it has one."""
    message = error["msg"].removeprefix(_VALUE_ERROR_PREFIX)
    field = ".".join(str(part) for part in error["loc"])
    return f"{field}: {message}" if field else message


_TRUNCATION_MARK = "..."


def truncated(value: Any, max_length: int) -> Any:
    """Return a text longer than `max_length` cut to it, ending in `...`; any other value as it is."""
    if isinstance(value, str) and len(value) > max_length:
        return value[: max_length - len(_TRUNCATION_MARK)] + _TRUNCATION_MARK
    return value


def empty_to_none(value: Any) -> Any:
    """Return None for an empty or blank string, any other value as it is."""
    return None if isinstance(value, str) and not value.strip() else value


def required(hint: str | None = None) -> Callable[[Any], Any]:
    """Return a validator rejecting None and empty or blank strings.

    :param hint: appended to the message, e.g. what to enter instead
    """
    message = f"is required, {hint}." if hint else "is required."

    def _required(value: Any) -> Any:
        """Reject None and empty or blank strings."""
        if empty_to_none(value) is None:
            raise ValueError(message)
        return value

    return _required


def matching(pattern: str, message: str) -> Callable[[str | None], str | None]:
    """Return a validator rejecting a string that does not match the pattern; None passes.

    :param message: the error message, as pydantic's one for `pattern=` only shows the regex

    The JSON schema does not show the pattern, so the field also needs
    `json_schema_extra={"pattern": pattern}`.
    """

    def _matching(value: str | None) -> str | None:
        """Reject a string that does not match the pattern."""
        # `fullmatch`, as `$` also matches before a trailing newline
        if value is not None and not re.fullmatch(pattern, value):
            raise ValueError(message)
        return value

    return _matching


RequiredStr = Annotated[str, pydantic.BeforeValidator(required())]
# None if empty or blank
OptionalStr = Annotated[str | None, pydantic.BeforeValidator(empty_to_none)]


def max_length(model: type[InputModel], field: str) -> int:
    """Return the maximum length declared for a string field of the input type.

    Raises:
        ValueError: if the field declares none.

    """
    limit = next(
        (
            constraint.max_length
            for constraint in model.model_fields[field].metadata
            if getattr(constraint, "max_length", None) is not None
        ),
        None,
    )
    if limit is None:
        raise ValueError(f"{model.__name__}.{field} declares no maximum length.")
    return limit
