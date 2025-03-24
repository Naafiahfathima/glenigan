import os

from bs4 import BeautifulSoup
from .logging_config import logger

from .file_downloader import FileDownloader
from .utils import sanitize_filename


class ApplicationHandler:
    """
    Processes individual application records.
    """

    def __init__(self, session, base_url: str, download_path: str, download_folder: str):
        self.session = session
        self.base_url = base_url
        self.download_path = download_path
        self.download_folder = download_folder
        self.downloader = FileDownloader(
            session, base_url, download_path, download_folder)

    def process_record(self, record: dict) -> None:
        """
        Process a single application record: fetch document page, then download files.
        """
        reference = record.get("reference", "").strip()
        # New: retrieve council code from record
        council_code = record.get("council_code", "").strip()
        markup = record.get("markup", "").lower()
        document_url = record.get("document_url", "").strip()

        logger.info(
            "Processing application with reference: %s, council code: %s, markup: %s",
            reference, council_code, markup
        )

        sanitized_ref = sanitize_filename(reference)
        # Sanitize the council code as well
        sanitized_council = sanitize_filename(council_code)

        # Build the folder path: download_folder / council_code / application reference
        app_folder = os.path.join(
            self.download_folder, sanitized_council, sanitized_ref)
        if not os.path.exists(app_folder):
            try:
                os.makedirs(app_folder)
                logger.info("Created application folder: %s", app_folder)
            except OSError as e:
                logger.error(
                    "Failed to create application folder %s: %s", app_folder, e)
                return

        if not document_url:
            logger.error(
                "No document URL provided for case %s. Skipping.", reference)
            return

        logger.info(
            "Retrieving document page for case %s from URL: %s", reference, document_url)
        try:
            resp = self.session.get(document_url, timeout=10)
        except Exception as e:
            logger.error(
                "HTTP request failed for document URL %s: %s", document_url, e)
            return

        if resp.status_code != 200:
            logger.error(
                "Case %s: Failed to retrieve document page. HTTP status: %s", reference, resp.status_code)
            return

        soup = BeautifulSoup(resp.content, "html.parser")
        input_tags = soup.find_all("input", {"name": "file"})
        logger.info(
            "Found %d file input(s) on the document page.", len(input_tags))

        if markup == "minor":
            logger.info(
                "Case %s: Markup is 'minor'. No downloads will be performed.", reference)
            return
        elif markup == "house":
            for tag in input_tags:
                value = tag.get("value", "")
                if "APPLICATION_FORM" in value.upper():
                    logger.info(
                        "Found application form for HOUSE category: %s", value)
                    self.downloader.download_file(value, reference, app_folder)
                    break
            else:
                logger.warning(
                    "Case %s: Application form not found on the document page.", reference)
        elif markup == "smallsandlarge":
            for tag in input_tags:
                value = tag.get("value", "")
                if value:
                    logger.info(
                        "Downloading file for SMALLSANDLARGE category: %s", value)
                    self.downloader.download_file(value, reference, app_folder)
        else:
            logger.warning(
                "Case %s: Unknown markup type '%s'.", reference, markup)
