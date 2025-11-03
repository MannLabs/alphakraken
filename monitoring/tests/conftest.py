"""Shared configuration for monitoring tests."""

import sys
from pathlib import Path

# Add monitoring directory to sys.path so that 'alerts' and 'messenger_clients' can be imported
# This mimics the runtime behavior where PYTHONPATH includes the monitoring directory
monitoring_dir = Path(__file__).parent.parent
if str(monitoring_dir) not in sys.path:
    sys.path.insert(0, str(monitoring_dir))

# Webhook URLs are now configured via YAML settings.
# Test configuration is handled in shared/yamlsettings.py when ENV_NAME="_test_"
