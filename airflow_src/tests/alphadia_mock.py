"""A mock to test the airflow DAGs. Copy it to the cluster and reference it in your test script."""

import logging
import sys
from time import sleep

for arg in sys.argv:
    logging.info(f"got arg: {arg}")

sleep(10)

# TODO: check if referenced files are accessible
