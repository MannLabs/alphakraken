"""Shared configuration for monitoring tests."""

import os

# Set required environment variables for testing
os.environ["MESSENGER_WEBHOOK_URL"] = "http://test-webhook.example.com"
