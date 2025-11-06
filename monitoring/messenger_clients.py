"""Clients for sending messages to Slack or MS Teams."""

import json
import logging
import os

import requests

from shared.keys import EnvVars
from shared.yamlsettings import YamlKeys, get_notification_setting


class AlertTypes:
    """Types of alert messages."""

    ALERT = "alert"
    INFO = "info"


def send_message(
    message: str, webhook_url: str, message_type: str = AlertTypes.ALERT
) -> None:
    """Send message to Slack or MS Teams.

    Args:
        message: The message to send
        webhook_url: Webhook URL to send to.
        message_type: Type of message - 'alert' (default) or 'info'

    """
    # TODO: this could be more elegant
    logging.info(f"Sending message: {message}")
    try:
        hostname = get_notification_setting(YamlKeys.HOSTNAME)
    except KeyError:
        logging.warning("HOSTNAME not found in config, using empty string")
        hostname = ""

    if webhook_url.startswith("https://hooks.slack.com"):
        _send_slack_message(message, hostname, webhook_url, message_type)
    else:
        _send_msteams_message(message, hostname, webhook_url)


def _send_slack_message(
    message: str, hostname: str, webhook_url: str, message_type: str = "alert"
) -> None:
    env_name = os.environ.get(EnvVars.ENV_NAME)

    if message_type == AlertTypes.INFO:
        emoji = "ℹ️"  # noqa: RUF001
        label = "Info"
        prefix = ""
    else:  # alert
        emoji = "🚨"
        label = "Alert"
        prefix = "<!channel> " if env_name == "production" else ""

    payload = {
        "text": f"{emoji} {prefix}[{env_name}] *{label}*: {message} (sent from {hostname})",
    }
    response = requests.post(webhook_url, json=payload, timeout=10)
    response.raise_for_status()
    logging.info("Successfully sent Slack message.")


def _send_msteams_message(message: str, hostname: str, webhook_url: str) -> None:
    # Define the adaptive card JSON
    message = f"{message} (sent from {hostname})"
    adaptive_card_json = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "body": [{"type": "TextBlock", "text": message}],
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
