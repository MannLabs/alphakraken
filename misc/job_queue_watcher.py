#!/usr/bin/env python3
"""Simple file watcher for processing .job files and simulating job execution when using the "file_based" job engine.

See also the `FileBasedJobHandler` class.

This script watches for .job files in a specified directory and processes them by:
1. Reading the KEY=value environment from the .job file
2. Creating the output directory
3. Writing job_status.log with simulation logs and final status
4. Moving the .job file to processed state

Usage:
    python simple_file_watcher.py [watch_directory]

IMPORTANT NOTE: Make sure as few as possible external libraries are used to ensure portability.
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path


def setup_logging() -> None:
    """Setup basic logging configuration."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def parse_job_file(job_file_path: Path) -> dict[str, str]:
    """Parse a .job file and return the environment as a dictionary.

    Args:
        job_file_path: Path to the .job file

    Returns:
        Dictionary of environment variables

    """
    environment = {}

    try:
        with job_file_path.open("r") as f:
            for line_ in f:
                line = line_.strip()
                if line and "=" in line:
                    key, value = line.split("=", 1)
                    environment[key] = value
    except Exception:
        logging.exception(f"Failed to parse job file {job_file_path}.")

    return environment


def simulate_job_execution(environment: dict[str, str], output_path: Path) -> None:
    """Simulate job execution and write status to job_status.log.

    Args:
        environment: Environment variables from the .job file
        output_path: Path where job_status.log should be written

    """
    status_file = output_path / "job_status.log"

    try:
        # Ensure output directory exists
        output_path.mkdir(parents=True, exist_ok=True)

        with status_file.open("w") as f:
            start_time = datetime.now()  # noqa: DTZ005
            f.write(f"Starting at {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.flush()

            # Simulate job steps
            job_steps = [
                "Initializing job environment...",
                f"Loading raw file: {environment.get('RAW_FILE_PATH', 'N/A')}",
                f"Using settings from: {environment.get('SETTINGS_PATH', 'N/A')}",
                f"Software: {environment.get('SOFTWARE', 'N/A')}",
                "Processing data...",
                "Running analysis...",
                "Generating outputs...",
            ]

            for _, step in enumerate(job_steps):
                f.write(f"[{datetime.now().strftime('%H:%M:%S')}] {step}\n")  # noqa: DTZ005
                f.flush()
                time.sleep(0.5)  # Simulate processing time

            # Simulate success/failure (90% success rate for testing)
            import random

            success = random.random() < 0.9  # noqa: PLR2004, S311

            if success:
                f.write(
                    f"[{datetime.now().strftime('%H:%M:%S')}] Job completed successfully\n"  # noqa: DTZ005
                )
                f.write("COMPLETED\n")
                logging.info(
                    f"Job completed successfully for {environment.get('RAW_FILE_ID', 'unknown')}"
                )
            else:
                f.write(
                    f"[{datetime.now().strftime('%H:%M:%S')}] Job failed with error\n"  # noqa: DTZ005
                )
                f.write("FAILED\n")
                logging.info(
                    f"Job failed for {environment.get('RAW_FILE_ID', 'unknown')}"
                )

    except Exception as e:
        logging.exception("Failed to simulate job execution.")
        # Write failure status if we can
        with status_file.open("w") as f:
            f.write(f"Starting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")  # noqa: DTZ005
            f.write(f"Error during job execution: {e}\n")
            f.write("FAILED\n")


def process_job_file(job_file_path: Path) -> None:
    """Process a single .job file.

    Args:
        job_file_path: Path to the .job file to process

    """
    logging.info(f"Processing job file: {job_file_path}")

    # Parse the job file
    environment = parse_job_file(job_file_path)

    if not environment:
        logging.error(f"Failed to parse environment from {job_file_path}")
        return

    logging.info("Got environment:")
    for key, value in environment.items():
        logging.info(f"  {key}={value}")

    # Get output path from environment
    output_path_str = environment.get("OUTPUT_PATH")
    if not output_path_str:
        logging.error(f"No OUTPUT_PATH found in {job_file_path}")
        return

    output_path = Path(output_path_str)

    # Simulate job execution
    simulate_job_execution(environment, output_path)

    # Move the job file to indicate it's been processed
    processed_file = job_file_path.with_suffix(".job.processed")
    try:
        job_file_path.rename(processed_file)
        logging.info(f"Job file moved to: {processed_file}")
    except Exception:
        logging.exception("Failed to move job file.")


def watch_directory(watch_dir: Path) -> None:
    """Watch a directory for .job files and process them.

    Args:
        watch_dir: Directory to watch for .job files

    """
    logging.info(f"Watching directory: {watch_dir}")

    if not watch_dir.exists():
        logging.error(f"Watch directory does not exist: {watch_dir}")
        return

    processed_files = set()

    try:
        while True:
            # Find all .job files
            job_files = list(watch_dir.glob("*.job"))

            for job_file in job_files:
                if job_file not in processed_files:
                    process_job_file(job_file)
                    processed_files.add(job_file)

            # Sleep before next check
            time.sleep(30)

    except KeyboardInterrupt:
        logging.info("File watcher stopped by user")


def main() -> int:
    """Main entry point for the file watcher."""
    setup_logging()

    parser = argparse.ArgumentParser(description="Simple file watcher for .job files")
    parser.add_argument(
        "watch_dir", nargs="?", help="Directory to watch for .job files"
    )
    parser.add_argument(
        "--once", action="store_true", help="Process existing files once and exit"
    )

    args = parser.parse_args()

    # Determine watch directory
    watch_dir = Path(args.watch_dir)

    logging.info(f"Using watch directory: {watch_dir}")

    if args.once:
        # Process existing files once and exit
        job_files = list(watch_dir.glob("*.job"))
        if not job_files:
            logging.info("No .job files found")
        else:
            for job_file in job_files:
                process_job_file(job_file)
            logging.info(f"Processed {len(job_files)} job files")
    else:
        # Continuous watching
        watch_directory(watch_dir)

    return 0


if __name__ == "__main__":
    sys.exit(main())
