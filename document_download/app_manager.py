import requests
from .logging_config import logger

from .app_handler import ApplicationHandler
from .db_connector import DatabaseConfigLoader, get_connection


class AppManager:
    """
    Coordinates fetching records from the database and processing application records.
    """

    def __init__(self, db_config_path: str, db_query: str, base_url: str,
                 download_path: str, download_folder: str):
        self.db_config_path = db_config_path
        self.db_query = db_query
        self.base_url = base_url
        self.download_path = download_path
        self.download_folder = download_folder
        self.session = requests.Session()
        self.db_config = DatabaseConfigLoader.load_config(db_config_path)

    def run(self) -> None:
        """
        Connect to the database, fetch records, and process each application.
        """
        try:
            conn = get_connection(self.db_config)
            logger.info("Successfully connected to the database.")
        except Exception as e:
            logger.error("Could not connect to the database: %s", e)
            return

        try:
            with conn.cursor() as cursor:
                cursor.execute(self.db_query)
                records = cursor.fetchall()
                logger.info(
                    "Fetched %d record(s) from the database.", len(records))
                if not records:
                    logger.warning("No application records found. Exiting.")
                    return

                handler = ApplicationHandler(self.session, self.base_url,
                                             self.download_path, self.download_folder)
                for record in records:
                    handler.process_record(record)
        finally:
            conn.close()
            logger.info("Database connection closed.")
