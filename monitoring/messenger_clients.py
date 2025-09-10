"""Clients for sending messages to Slack or MS Teams."""

import json
import logging
import os

import requests
from config import MESSENGER_WEBHOOK_URL

from shared.keys import EnvVars


def send_message(message: str) -> None:
    """Send message to Slack or MS Teams."""
    # TODO: this could be more elegant
    logging.info(f"Sending message: {message}")
    if MESSENGER_WEBHOOK_URL.startswith("https://hooks.slack.com"):
        _send_slack_message(message)
    else:
        _send_msteams_message(message)


def _send_slack_message(message: str) -> None:
    env_name = os.environ.get(EnvVars.ENV_NAME)

    prefix = "🚨 <!channel> " if env_name == "production" else ""
    payload = {
        "text": f"{prefix} [{env_name}] *Alert*: {message}",
    }
    response = requests.post(MESSENGER_WEBHOOK_URL, json=payload, timeout=10)
    response.raise_for_status()
    logging.info("Successfully sent Slack message.")


def _send_msteams_message(message: str) -> None:
    # Define the adaptive card JSON
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
        MESSENGER_WEBHOOK_URL,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        data=json.dumps(adaptive_card_json),
        timeout=10,
    )
    response.raise_for_status()
    logging.info("Successfully sent MS Teams message.")
