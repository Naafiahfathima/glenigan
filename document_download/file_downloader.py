import os
import zipfile

import requests
from .logging_config import logger


class FileDownloader:
    """
    Handles downloading of files and ZIP extraction.
    """

    def __init__(self, session: requests.Session, base_url: str,
                 download_path: str, download_folder: str):
        self.session = session
        self.base_url = base_url.rstrip('/')
        self.download_path = download_path.rstrip('/')
        self.download_folder = download_folder

    def download_file(self, file_path: str, case_number: str, app_folder: str) -> None:
        """
        Download a file from a given URL and extract it if it's a ZIP archive.
        """
        if not os.path.exists(app_folder):
            try:
                os.makedirs(app_folder)
                logger.info("Created folder: %s", app_folder)
            except OSError as e:
                logger.error("Failed to create folder %s: %s", app_folder, e)
                return

        file_url = f"{self.base_url}{self.download_path}/?file={file_path}&caseNumber={case_number}"
        logger.info("Downloading file from URL: %s", file_url)
        try:
            response = self.session.get(file_url, timeout=10)
        except requests.RequestException as e:
            logger.error("HTTP request failed for file %s: %s", file_url, e)
            return

        if response.status_code == 200:
            filename = os.path.basename(file_path)
            file_full_path = os.path.join(app_folder, filename)
            try:
                with open(file_full_path, "wb") as f:
                    f.write(response.content)
                logger.info("Downloaded %s for case %s to %s",
                            filename, case_number, file_full_path)
            except IOError as e:
                logger.error("Failed to write file %s: %s", file_full_path, e)
                return

            if zipfile.is_zipfile(file_full_path):
                logger.info(
                    "%s is a ZIP archive. Extracting contents...", filename)
                extract_folder = os.path.join(
                    app_folder, os.path.splitext(filename)[0])
                try:
                    os.makedirs(extract_folder, exist_ok=True)
                    with zipfile.ZipFile(file_full_path, 'r') as zip_ref:
                        zip_ref.extractall(extract_folder)
                    os.remove(file_full_path)
                    logger.info(
                        "Extracted files to %s and deleted ZIP archive", extract_folder)
                except (IOError, zipfile.BadZipFile) as e:
                    logger.error(
                        "Failed to extract ZIP file %s: %s", file_full_path, e)
            else:
                logger.info(
                    "%s is not a ZIP archive. No extraction needed.", filename)
        else:
            logger.error("Failed to download file for case %s from URL: %s. HTTP status: %s",
                         case_number, file_url, response.status_code)
