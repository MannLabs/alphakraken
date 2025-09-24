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
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# if not none, the output path will constructed as OUTPUT_PATH_BASE / RELATIVE_OUTPUT_PATH
OUTPUT_PATH_BASE: str | None = None  # "//192.168.0.1"


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


def execute_job_process(environment: dict[str, str], output_path: Path) -> None:
    """Execute a real job process and write status to job_status.log.

    Args:
        environment: Environment variables from the .job file
        output_path: Path where job_status.log should be written

    """
    status_file = output_path / "job_status.log"
    raw_file_id = environment.get("RAW_FILE_ID", "unknown")

    try:
        # Ensure output directory exists
        output_path.mkdir(parents=True, exist_ok=True)

        # Determine the command to execute
        custom_command = environment.get("CUSTOM_COMMAND", "").strip()

        logging.info(f"Executing command for {raw_file_id}: {custom_command}")

        with status_file.open("w", buffering=1) as f:  # Line buffered
            start_time = datetime.now()  # noqa: DTZ005
            f.write(f"Starting at {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(
                f"[{datetime.now().strftime('%H:%M:%S')}] Executing: {custom_command}\n"  # noqa: DTZ005
            )
            f.flush()

            # Execute the command with cross-platform compatibility
            process = subprocess.Popen(  # noqa: S602 # `subprocess` call with `shell=True` identified, security issue
                custom_command,
                shell=True,  # Required for Windows compatibility and complex commands
                # cwd=str(output_path),  # Set working directory to output path
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # Merge stderr with stdout
                text=True,
                bufsize=1,  # Line buffered
                universal_newlines=True,
            )

            # Stream output in real-time
            while True:
                output = process.stdout.readline()
                if output == "" and process.poll() is not None:
                    break
                if output:
                    timestamp = datetime.now().strftime("%H:%M:%S")  # noqa: DTZ005
                    f.write(f"[{timestamp}] {output}")
                    f.flush()

            # Wait for process to complete and get return code
            return_code = process.wait()

            # Write final status based on return code
            timestamp = datetime.now().strftime("%H:%M:%S")  # noqa: DTZ005
            if return_code == 0:
                f.write(
                    f"[{timestamp}] Process completed successfully (exit code: {return_code})\n"
                )
                f.write("COMPLETED\n")
                logging.info(f"Job completed successfully for {raw_file_id}")
            else:
                f.write(f"[{timestamp}] Process failed with exit code: {return_code}\n")
                f.write("FAILED\n")
                logging.info(
                    f"Job failed for {raw_file_id} with exit code: {return_code}"
                )

    except subprocess.SubprocessError as e:
        logging.exception(f"Subprocess error during job execution for {raw_file_id}")
        with status_file.open("w") as f:
            f.write(f"Starting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")  # noqa: DTZ005
            f.write(f"Subprocess error: {e}\n")
            f.write("FAILED\n")
    except Exception as e:
        logging.exception(f"Failed to execute job process for {raw_file_id}")
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
    if OUTPUT_PATH_BASE:
        relative_output_path = environment["OUTPUT_PATH"]
        output_path_str = f"{OUTPUT_PATH_BASE}\\{relative_output_path})"
    else:
        output_path_str = environment["OUTPUT_PATH"]

    output_path = Path(output_path_str)

    # Execute real job process
    execute_job_process(environment, output_path)

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
            logging.info("Checking for new .job files...")

            job_files = list(watch_dir.glob("*.job"))

            for job_file in job_files:
                if job_file not in processed_files:
                    try:
                        process_job_file(job_file)
                        processed_files.add(job_file)
                    except KeyboardInterrupt:
                        raise
                    except Exception:
                        logging.exception("Error processing job files.")
                        processed_file = job_file.with_suffix(".job.error.processed")
                        job_file.rename(processed_file)

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

    args = parser.parse_args()

    # Determine watch directory
    watch_dir = Path(args.watch_dir)

    logging.info(f"Using watch directory: {watch_dir}")

    # Continuous watching
    watch_directory(watch_dir)

    return 0


if __name__ == "__main__":
    sys.exit(main())
