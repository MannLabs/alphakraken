"""Shared code for all tests."""

import os

os.environ["ENV_NAME"] = "_test_"
# importing the server module must not start the server
os.environ["MCP_TRANSPORT"] = "disabled"
