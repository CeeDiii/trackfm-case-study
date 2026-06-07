"""Download and extract the Last.fm 1K dataset."""
import logging
import tarfile

import requests

from utils.constants import BASE_DIR
from utils.logging import configure_logging

logger = logging.getLogger(__name__)

DATA_DIR = BASE_DIR / "data"

BASE_URL = "http://mtg.upf.edu/static/datasets/last.fm"
FILE_NAME = "lastfm-dataset-1K.tar.gz"
DOWNLOAD_URL = f"{BASE_URL}/{FILE_NAME}"


def download() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_file = DATA_DIR / FILE_NAME

    if output_file.exists():
        logger.info("Already downloaded: %s", output_file)
        return output_file

    logger.info("Downloading %s ...", DOWNLOAD_URL)
    with requests.get(DOWNLOAD_URL, stream=True) as response:
        response.raise_for_status()
        content_length = response.headers.get("Content-Length")
        total_mb = round(int(content_length) / 1_000_000) if content_length else None
        if total_mb:
            logger.info("Total file size: %d MB", total_mb)
        else:
            logger.warning("Content-Length header missing — file size unknown")

        with open(output_file, "wb") as f:
            chunk_size = 1024 * 1024
            for i, chunk in enumerate(response.iter_content(chunk_size=chunk_size)):
                f.write(chunk)
                downloaded_mb = round(i * chunk_size / 1_000_000)
                if downloaded_mb == 0 or downloaded_mb % 50 == 0:
                    total_str = str(total_mb) if total_mb else "unknown"
                    logger.info("Progress: %d MB / %s MB", downloaded_mb, total_str)

    logger.info("Downloaded to %s", output_file)
    return output_file


def extract(archive) -> None:
    extracted_dir = DATA_DIR / "lastfm-dataset-1K"
    if extracted_dir.exists():
        logger.info("Already extracted: %s", extracted_dir)
        return

    with tarfile.open(archive, "r:gz") as tar:
        logger.info("Extracting %s ...", archive.name)
        tar.extractall(path=DATA_DIR)
        logger.info("Extracted to %s", DATA_DIR)


if __name__ == "__main__":
    configure_logging()
    archive = download()
    extract(archive)
