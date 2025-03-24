from document_download.logging_config import logger

from document_download.app_manager import AppManager
from document_download.settings import Settings
from document_download.utils import flatten_all_documents


def main() -> None:
    """
    Main entry point for the application. Loads configuration, runs the manager,
    and flattens the downloaded document folders.
    """
    # Load and validate configuration
    config = Settings()
    try:
        config.validate()
    except ValueError as e:
        logger.error(e)
        return

    manager = AppManager(
        db_config_path=config.db_config_path,
        db_query=config.db_query,
        base_url=config.base_url,
        download_path=config.download_path,
        download_folder=config.download_folder
    )
    manager.run()

    # Flatten downloaded document folders
    flatten_all_documents(config.download_folder)
    logger.info("All document folders flattened.")


if __name__ == "__main__":
    logger.info("Starting application execution...")
    main()
    logger.info("Application execution completed.")
