"""Messenger facade: sends messages to channels and users, hiding the platform (Slack or MS Teams)."""

import json
import logging
import os
from collections.abc import Callable, Collection
from dataclasses import dataclass
from functools import partial

import requests
from alerts import config
from alerts.config import Destinations

from shared.keys import EnvVars

_WEBHOOK_URLS = {
    Destinations.OPS: config.OPS_ALERTS_WEBHOOK_URL,
    Destinations.BUSINESS: config.BUSINESS_ALERTS_WEBHOOK_URL,
}
_REQUEST_TIMEOUT_S = 10
_SLACK_POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage"
# Slack API errors that a retry cannot fix, e.g. a stale user id
_SLACK_PERMANENT_ERRORS = {
    "user_not_found",
    "users_not_found",
    "channel_not_found",
    "not_in_channel",
    "account_inactive",
    "invalid_auth",
}


class PermanentSendError(Exception):
    """A send failed for a reason that a retry cannot fix."""


class AlertTypes:
    """Types of alert messages."""

    ALERT = "alert"
    INFO = "info"


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

    Every send is attempted; return whether all succeeded. A send that failed permanently
    counts as done, as retrying it would fail again.
    """
    all_sent = True
    # set: the business webhook URL falls back to the ops one if not configured
    for webhook_url in {_WEBHOOK_URLS[destination] for destination in destinations}:
        all_sent &= _attempt(
            partial(_send_channel_message, message, webhook_url), "channel message"
        )

    for recipient in recipients:
        all_sent &= _attempt(
            partial(_send_direct_message, message, recipient),
            f"direct message to {recipient}",
        )

    return all_sent


def _attempt(send_function: Callable[[], None], what: str) -> bool:
    """Call the send function, log a failure and return whether it succeeded."""
    try:
        send_function()
    except PermanentSendError as e:
        logging.warning(f"Not retrying {what}: {e}")
    except Exception:
        logging.exception(f"Failed to send {what}")
        return False
    return True


def _text(message: Message) -> str:
    """Render title and body as one text."""
    if message.title:
        return f"{message.title}:\n{message.body}"
    return message.body


def _send_channel_message(message: Message, webhook_url: str) -> None:
    """Send message to a Slack or MS Teams channel."""
    logging.info(f"Sending message: {_text(message)}")
    hostname = os.environ.get(EnvVars.KRAKEN_HOSTNAME, "")

    if webhook_url.startswith("https://hooks.slack.com"):
        _send_channel_message_slack(message, hostname, webhook_url)
    else:
        _send_channel_message_msteams(message, hostname, webhook_url)


def _send_channel_message_slack(
    message: Message, hostname: str, webhook_url: str
) -> None:
    env_name = os.environ.get(EnvVars.ENV_NAME)

    if message.type == AlertTypes.INFO:
        emoji = "ℹ️"  # noqa: RUF001
        label = "Info"
        prefix = ""
    else:  # alert
        emoji = "🚨"
        label = "Alert"
        prefix = "<!channel> " if env_name == "production" else f"[{env_name}] "

    payload = {
        "text": f"{emoji} {prefix}*{label}*: {_text(message)} (sent from {hostname})",
    }
    response = requests.post(webhook_url, json=payload, timeout=_REQUEST_TIMEOUT_S)
    response.raise_for_status()
    logging.info("Successfully sent Slack message.")


def _send_channel_message_msteams(
    message: Message, hostname: str, webhook_url: str
) -> None:
    text = f"{_text(message)} (sent from {hostname})"
    adaptive_card_json = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "body": [{"type": "TextBlock", "text": text}],
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.0",
                },
            }
        ],
    }

    response = requests.post(
        webhook_url,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        data=json.dumps(adaptive_card_json),
        timeout=_REQUEST_TIMEOUT_S,
    )
    response.raise_for_status()
    logging.info("Successfully sent MS Teams message.")


def _send_direct_message(message: Message, recipient_id: str) -> None:
    """Send a direct message to a user.

    Logs a warning and returns when no DM credentials are configured.
    """
    if config.SLACK_BOT_TOKEN:
        _send_direct_message_slack(message, recipient_id, config.SLACK_BOT_TOKEN)
        return

    logging.warning(
        f"No DM credentials configured; dropping message to recipient {recipient_id}."
    )


def _send_direct_message_slack(message: Message, user_id: str, token: str) -> None:
    env_name = os.environ.get(EnvVars.ENV_NAME)

    if message.type == AlertTypes.INFO:
        emoji = "ℹ️"  # noqa: RUF001
        prefix = ""
    else:
        emoji = "🚨"
        prefix = "" if env_name == "production" else f"[{env_name}] "

    payload = {"channel": user_id, "text": f"{emoji} {prefix}{_text(message)}"}
    response = requests.post(
        _SLACK_POST_MESSAGE_URL,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        json=payload,
        timeout=_REQUEST_TIMEOUT_S,
    )
    response.raise_for_status()
    body = response.json()
    if not body.get("ok"):
        if body.get("error") in _SLACK_PERMANENT_ERRORS:
            raise PermanentSendError(f"Slack chat.postMessage failed: {body!r}")
        raise RuntimeError(f"Slack chat.postMessage failed: {body!r}")
    logging.info(f"Successfully sent Slack DM to {user_id}.")
