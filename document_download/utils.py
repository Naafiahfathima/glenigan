import os
import re
import shutil

from .logging_config import logger


def sanitize_filename(filename: str) -> str:
    """
    Sanitize the filename by replacing illegal characters with underscores.
    """
    safe = re.sub(r'[^\w\-_\. ]', '_', filename)
    logger.debug("Sanitized filename: %s", safe)
    return safe


def flatten_application_folder(app_folder: str) -> None:
    """
    Flatten the folder structure for a single application by moving files
    from subdirectories into the main folder and removing empty directories.
    """
    logger.info("Flattening folder: %s", app_folder)
    for root, dirs, files in os.walk(app_folder, topdown=False):
        if os.path.abspath(root) == os.path.abspath(app_folder):
            continue
        for file in files:
            src = os.path.join(root, file)
            dest = os.path.join(app_folder, file)
            try:
                shutil.move(src, dest)
                logger.info("Moved file: %s -> %s", src, dest)
            except Exception as e:
                logger.error("Failed to move file %s: %s", src, e)
        try:
            os.rmdir(root)
            logger.info("Deleted empty folder: %s", root)
        except Exception as e:
            logger.warning("Could not delete folder %s: %s", root, e)


def flatten_all_documents(documents_folder: str) -> None:
    logger.info("Flattening all documents in: %s", documents_folder)
    # Loop over council code directories
    for council in os.listdir(documents_folder):
        council_path = os.path.join(documents_folder, council)
        if os.path.isdir(council_path):
            # Loop over each application folder within the council folder
            for app in os.listdir(council_path):
                app_folder = os.path.join(council_path, app)
                if os.path.isdir(app_folder):
                    flatten_application_folder(app_folder)
