import os

from dotenv import load_dotenv


class Settings:
    """
    Loads and validates configuration from environment variables.
    """

    def __init__(self):
        load_dotenv()
        self.db_config_path = os.getenv("DB_CONFIG_PATH")
        self.base_url = os.getenv("BASE_URL")
        self.download_folder = os.getenv("DOWNLOAD_FOLDER", "documents")
        self.db_query = os.getenv("DB_QUERY", "SELECT * FROM plan_data")
        self.download_path = os.getenv(
            "DOWNLOAD_PATH", "/online-applications/download/")

    def validate(self):
        if not self.db_config_path or not self.base_url:
            raise ValueError(
                "Environment variables DB_CONFIG_PATH and BASE_URL must be set.")
