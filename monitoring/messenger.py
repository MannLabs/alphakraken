"""Messenger facade: sends messages to channels and users through apprise, hiding the platform."""

import logging
import os
from collections.abc import Collection
from dataclasses import dataclass

import apprise
from alerts import config
from alerts.config import Destinations

from shared.keys import EnvVars

_CHANNEL_URLS = {
    Destinations.OPS: config.OPS_ALERTS_URL,
    Destinations.BUSINESS: config.BUSINESS_ALERTS_URL,
}
# services without a footer ignore this apprise URL argument
_NO_FOOTER_ARG = "footer=no"


class AlertTypes:
    """Types of alert messages."""

    ALERT = "alert"
    INFO = "info"


_NOTIFY_TYPES = {
    AlertTypes.ALERT: apprise.NotifyType.FAILURE,
    AlertTypes.INFO: apprise.NotifyType.INFO,
}


@dataclass(frozen=True)
class Message:
    """A message to send, independent of the messenger platform."""

    body: str
    title: str | None = None
    type: str = AlertTypes.ALERT


def send(
    message: Message,
    destinations: Collection[str] = (),
    recipients: Collection[str] = (),
) -> bool:
    """Send the message to the channels of the destinations and directly to the recipients.

    Every send is attempted; return whether all succeeded. Recipients are dropped with a
    warning when no user alerts URL is configured.
    """
    all_sent = True
    # set: the business URL falls back to the ops one if not configured
    for url in {_CHANNEL_URLS[destination] for destination in destinations}:
        all_sent &= _notify(url, message, "channel message")

    if recipients and not config.USER_ALERTS_URL:
        logging.warning(
            f"No user alerts URL configured; dropping messages to {list(recipients)}."
        )
        return all_sent

    for recipient in recipients:
        url = config.USER_ALERTS_URL.replace(config.MESSENGER_ID_PLACEHOLDER, recipient)
        all_sent &= _notify(url, message, f"direct message to {recipient}")

    return all_sent


def _notify(url: str, message: Message, what: str) -> bool:
    """Send the message to the apprise URL, log a failure and return whether it succeeded."""
    body = _body(message)
    logging.info(f"Sending {what}: {message.title}: {body}")

    notifier = apprise.Apprise()
    # the URL holds credentials, so it is not logged
    if not notifier.add(f"{url}{'&' if '?' in url else '?'}{_NO_FOOTER_ARG}"):
        logging.error(f"Invalid notification URL for {what}")
        return False

    sent = bool(
        notifier.notify(
            title=message.title or "",
            body=body,
            notify_type=_NOTIFY_TYPES[message.type],
        )
    )
    if not sent:
        logging.error(f"Failed to send {what}")
    return sent


def _body(message: Message) -> str:
    """Render the body with the environment prefix of non-production alerts and the sender."""
    env_name = os.environ.get(EnvVars.ENV_NAME)
    hostname = os.environ.get(EnvVars.KRAKEN_HOSTNAME, "")
    prefix = ""
    if message.type == AlertTypes.ALERT and env_name != "production":
        prefix = f"[{env_name}] "
    return f"{prefix}{message.body}\n(sent from {hostname})"
