"""Download the demo raw files from MPIB datashare into misc/demo/raw_files.

Requires `pip install alphabase progressbar2`. Runs on the host, not in a container.
"""

from pathlib import Path

from alphabase.tools.data_downloader import DataShareDownloader

_SHARE_URL = "https://datashare.biochem.mpg.de/public.php/dav/files/WTu3rFZHNeb3uG2"

# The file names contain 'ADIAMA' as an underscore-separated token, which is what associates them
# with the demo project seeded by seed_db.py.
RAW_FILE_NAMES = [
    "20231024_OA3_TiHe_ADIAMA_HeLa_200ng_Evo01_21min_F-40_iO_before_01.raw",
    "20231024_OA3_TiHe_ADIAMA_HeLa_200ng_Evo01_21min_F-40_iO_before_02.raw",
    "20231024_OA3_TiHe_ADIAMA_HeLa_200ng_Evo01_21min_F-40_iO_before_03.raw",
]

OUTPUT_DIR = Path(__file__).parent / "raw_files"


def main() -> None:
    """Download all demo raw files that are not present yet."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for file_name in RAW_FILE_NAMES:
        if (OUTPUT_DIR / file_name).exists():
            print(f"Already present, skipping: {file_name}")  # noqa: T201
            continue

        print(f"Downloading {file_name} ..")  # noqa: T201
        DataShareDownloader(
            url=f"{_SHARE_URL}/{file_name}", output_dir=str(OUTPUT_DIR)
        ).download()

    print(f"All demo raw files are in {OUTPUT_DIR}")  # noqa: T201


if __name__ == "__main__":
    main()
