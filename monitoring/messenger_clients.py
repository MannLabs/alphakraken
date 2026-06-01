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
        prefix = "<!channel> " if env_name == "production" else f"[{env_name}] "

    payload = {
        "text": f"{emoji} {prefix}*{label}*: {message} (sent from {hostname})",
    }
    response = requests.post(webhook_url, json=payload, timeout=10)
    response.raise_for_status()
    logging.info("Successfully sent Slack message.")


def send_dm(
    message: str, recipient_id: str, *, message_type: str = AlertTypes.ALERT
) -> None:
    """Send a direct message to a user.

    Dispatches to the appropriate platform implementation based on which
    credentials are configured in `alerts.config`. Logs a warning and
    returns (no raise) when no DM credentials are configured, mirroring
    `send_message`'s no-crash policy.

    Note: when multiple platforms' credentials are configured simultaneously,
    Slack wins by `if/elif` order. Add a configurable preference if a second
    DM platform actually lands.
    """
    from alerts import config

    if config.SLACK_BOT_TOKEN:
        _send_slack_dm(message, recipient_id, config.SLACK_BOT_TOKEN, message_type)
        return

    logging.warning(
        f"No DM credentials configured; dropping message to recipient {recipient_id}."
    )


def _send_slack_dm(
    message: str, user_id: str, token: str, message_type: str = AlertTypes.ALERT
) -> None:
    env_name = os.environ.get(EnvVars.ENV_NAME)

    if message_type == AlertTypes.INFO:
        emoji = "ℹ️"  # noqa: RUF001
        prefix = ""
    else:
        emoji = "🚨"
        prefix = "" if env_name == "production" else f"[{env_name}] "

    payload = {"channel": user_id, "text": f"{emoji} {prefix}{message}"}
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


def _send_msteams_dm(
    message: str,
    user_id: str,
    token: str,
    message_type: str = AlertTypes.ALERT,
) -> None:
    raise NotImplementedError("MS Teams DMs not yet supported")


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
