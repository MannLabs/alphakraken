#!/usr/bin/env python3
"""Simple file watcher for processing .job files and simulating job execution when using the "file_based" job engine.

See also the `FileBasedJobHandler` class.

This script watches for .job files in a specified directory and processes them by:
1. Reading the KEY=value environment from the .job file
2. Creating the output directory
3. Writing job_status.log with simulation logs and final status
4. Moving the .job file to processed state

Usage:
    python job_queue_watcher.py [watch_directory]

IMPORTANT NOTE: Make sure as few as possible external libraries are used to ensure portability.
"""

import argparse
import logging
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

# non-standard library imports
import psutil

# if not None, the output path will constructed as OUTPUT_PATH_BASE / RELATIVE_OUTPUT_PATH
OUTPUT_PATH_BASE: str | None = None  # r"\\192.168.0.2\sharedfs$\alphakraken\output"
DEFAULT_JOB_QUEUE_FOLDER: str | None = (
    None  # r"\\192.168.0.2\sharedfs$\alphakraken\output\job_queue"
)

PROGRAM_NAME: str | None = None  # e.g. "spectronaut.dll"
MAX_INSTANCES = 1  # maximum number of simultaneous instances of PROGRAM_NAME


def setup_logging() -> None:
    """Setup basic logging configuration to log to file in parent directory."""
    # Get log file path in parent directory of this script
    script_dir = Path(__file__).parent
    log_file = script_dir / "job_queue_watcher.log"

    # Configure logging to write to file with rotation
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode="a"),  # Append to log file
            logging.StreamHandler(),  # Also keep console output
        ],
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
    """Execute a job process using Windows batch script (non-blocking).

    Args:
        environment: Environment variables from the .job file
        output_path: Path where job_status.log should be written

    """
    status_log_file = (
        output_path / "job_status.log"
    )  # Note: this must match the file name in execute_job.bat/execute_job.sh
    raw_file_id = environment.get("RAW_FILE_ID", "unknown")

    # Archive existing log file if present (happens on job retry)
    if status_log_file.exists():
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")  # noqa: DTZ005
        archived_name = f"job_status_{timestamp}.log"
        archived_path = output_path / archived_name
        status_log_file.rename(archived_path)
        logging.info(f"Archived previous log to {archived_name}")

    try:
        # Ensure output directory exists
        output_path.mkdir(parents=True, exist_ok=True)

        # Determine the command to execute
        custom_command = environment.get("CUSTOM_COMMAND", "").strip()

        logging.info(f"Launching job for {raw_file_id}: {custom_command}")

        # Write initial status to log file
        with status_log_file.open("w") as f:
            start_time = datetime.now()  # noqa: DTZ005
            f.write(f"Starting at {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

        # Get path to appropriate script (Windows batch or Unix shell)
        script_dir = Path(__file__).parent
        if sys.platform == "win32":
            script_path = script_dir / "execute_job.bat"
            use_shell = True
            creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            raise NotImplementedError("This script currently supports only Windows.")  # noqa: TRY301
            script_path = script_dir / "execute_job.sh"
            use_shell = False
            creation_flags = 0

        # For Windows batch files, we need to handle quotes very carefully
        # Use a simple approach: write parameters to a temp file instead of command line
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".tmp", delete=False
        ) as tmp_file:
            tmp_file.write(f"{custom_command}\n")
            tmp_file.write(f"{output_path}\n")
            tmp_file.write(f"{raw_file_id}\n")
            temp_file_path = tmp_file.name

        # Pass only the temp file path to avoid command line parsing issues
        script_command = f'"{script_path}" "{temp_file_path}"'

        logging.info(f"Executing batch command: {script_command}")

        # Launch script asynchronously (non-blocking)
        process = subprocess.Popen(  # noqa: S603 # `subprocess` call: check for execution of untrusted input
            script_command,
            shell=use_shell,
            creationflags=creation_flags,
            # Don't capture stdout/stderr - let script handle logging
            stdout=None,
            stderr=None,
        )

        with status_log_file.open("w") as f:
            f.write(f"PID {process.pid}\n")

        logging.info(f"Job launched for {raw_file_id} with PID {process.pid}")

    except Exception as e:
        logging.exception(f"Failed to launch job process for {raw_file_id}")
        with status_log_file.open("w") as f:
            f.write(f"Starting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")  # noqa: DTZ005
            f.write(f"Error launching job: {e}\n")
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
        relative_output_path = environment["RELATIVE_OUTPUT_PATH"]
        # TODO: use Path also here?
        output_path_str = f"{OUTPUT_PATH_BASE}\\{relative_output_path}"
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
    logging.info(f"Checking for new .job files in {watch_dir}...")

    if not watch_dir.exists():
        logging.error(f"Watch directory does not exist: {watch_dir}")
        return

    job_files = sorted(
        watch_dir.glob("*.job"), key=lambda f: f.stat().st_mtime, reverse=True
    )
    logging.info(f"Found {len(job_files)} new jobs...")

    if PROGRAM_NAME:
        instances_running = find_processes_matching(PROGRAM_NAME)
        n_jobs_to_start = max(0, MAX_INSTANCES - len(instances_running))
        if len(instances_running) >= MAX_INSTANCES:
            logging.info(
                f"Maximum number of instances ({len(instances_running)}/{MAX_INSTANCES}) reached. Exiting..\n{instances_running}"
            )
            return
    else:
        n_jobs_to_start = len(job_files)

    for job_file in job_files[:n_jobs_to_start]:
        try:
            process_job_file(job_file)
        except Exception:  # noqa: PERF203
            logging.exception("Error processing job files.")
            processed_file = job_file.with_suffix(".job.error.processed")
            job_file.rename(processed_file)


def find_processes_matching(pattern: str) -> list[str]:
    """Find processes matching a given pattern in their command line."""
    matching_processes = []
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            cmdline = " ".join(proc.info["cmdline"]) if proc.info["cmdline"] else ""
            if pattern.lower() in cmdline.lower():
                matching_processes.append(
                    {
                        "pid": proc.info["pid"],
                        "name": proc.info["name"],
                        "cmdline": cmdline,
                    }
                )
        except (psutil.NoSuchProcess, psutil.AccessDenied):  # noqa: PERF203
            pass
    return matching_processes


def main() -> int:
    """Main entry point for the file watcher."""
    setup_logging()

    parser = argparse.ArgumentParser(description="Simple file watcher for .job files")
    parser.add_argument(
        "watch_dir",
        nargs="?",
        help="Directory to watch for .job files",
        default=DEFAULT_JOB_QUEUE_FOLDER,
    )

    args = parser.parse_args()

    watch_dir = Path(args.watch_dir)

    watch_directory(watch_dir)

    return 0


if __name__ == "__main__":
    sys.exit(main())
