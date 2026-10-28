"""Messenger facade: sends messages to channels and users through apprise, hiding the platform."""

import logging
import os
from collections.abc import Collection
from dataclasses import dataclass
from urllib.parse import unquote

import apprise
import requests
from alerts import config
from alerts.config import Destinations
from apprise.decorators import notify as apprise_notify

from shared.keys import EnvVars

_CHANNEL_URLS = {
    Destinations.OPS: config.OPS_ALERTS_URL,
    Destinations.BUSINESS: config.BUSINESS_ALERTS_URL,
}
# services without a footer ignore this apprise URL argument
_NO_FOOTER_ARG = "footer=no"
# custom apprise service posting top-level text to Slack, see _send_slack_text
SLACK_TEXT_SCHEME = "slacktext"
_SLACK_TEXT_TIMEOUT_S = 10
_SLACK_POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage"
_SLACK_TEXT_EMOJIS = {
    apprise.NotifyType.FAILURE.value: "🚨",
    apprise.NotifyType.INFO.value: "ℹ️",  # noqa: RUF001
}


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


@apprise_notify(on=SLACK_TEXT_SCHEME, name="Slack, full-width text")
def _send_slack_text(
    body: str, title: str, notify_type: str, meta: dict, *args, **kwargs
) -> bool:
    """Post the message as top-level text to a Slack channel or user through a bot token.

    Unlike apprise's own Slack service, which posts attachments, Slack renders this at full width.
    URL: `slacktext://<bot token>/<channel id>`, `/<channel name>` or `/@<user id>`; the bot must be
    a member of the channel.
    """
    del args, kwargs
    emoji = _SLACK_TEXT_EMOJIS.get(notify_type, "")
    text = f"{emoji} *{title}*\n{body}" if title else f"{emoji} {body}"
    # Slack accepts channel ids, bare channel names and user ids; apprise percent-encodes a `#`
    target = unquote(meta["fullpath"]).lstrip("/@#")

    try:
        response = requests.post(
            _SLACK_POST_MESSAGE_URL,
            headers={
                "Authorization": f"Bearer {meta['host']}",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={"channel": target, "text": text},
            timeout=_SLACK_TEXT_TIMEOUT_S,
        )
    except requests.RequestException:
        logging.exception(f"Slack request to {target} failed")
        return False

    sent = response.ok and _slack_api_ok(response)
    if not sent:
        logging.error(
            f"Slack rejected the message to {target}: HTTP {response.status_code}, {response.text}"
        )
    return sent


def _slack_api_ok(response: requests.Response) -> bool:
    """Return whether the Slack API accepted the request: it answers errors with HTTP 200 and ok=false."""
    try:
        return bool(response.json().get("ok"))
    except requests.JSONDecodeError:
        return False
