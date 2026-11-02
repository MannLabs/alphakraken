"""Clients for sending messages to Slack or MS Teams."""

import json
import logging
import os
from dataclasses import dataclass

import requests
from alerts import config

from shared.keys import EnvVars


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


def _text(message: Message) -> str:
    """Render title and body as one text."""
    if message.title:
        return f"{message.title}:\n{message.body}"
    return message.body


def send_channel_message(message: Message, webhook_url: str) -> None:
    """Send message to a Slack or MS Teams channel."""
    # TODO: this could be more elegant
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
    response = requests.post(webhook_url, json=payload, timeout=10)
    response.raise_for_status()
    logging.info("Successfully sent Slack message.")


def _send_channel_message_msteams(
    message: Message, hostname: str, webhook_url: str
) -> None:
    # Define the adaptive card JSON
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
        timeout=10,
    )
    response.raise_for_status()
    logging.info("Successfully sent MS Teams message.")


def send_direct_message(message: Message, recipient_id: str) -> None:
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
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        json=payload,
        timeout=10,
    )
    response.raise_for_status()
    body = response.json()
    if not body.get("ok"):
        raise RuntimeError(f"Slack chat.postMessage failed: {body!r}")
    logging.info(f"Successfully sent Slack DM to {user_id}.")
