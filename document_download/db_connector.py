import configparser

import pymysql
from .logging_config import logger


class DatabaseConfigLoader:
    """
    Load database configuration from a .ini file.
    """
    @staticmethod
    def load_config(path: str) -> dict:
        config = configparser.ConfigParser()
        config.read(path)
        logger.info("Loaded database config from %s", path)
        return {
            "host": config["mysql"]["host"],
            "user": config["mysql"]["user"],
            "password": config["mysql"]["password"],
            "database": config["mysql"]["database"],
            "port": int(config["mysql"]["port"])
        }


def get_connection(db_config: dict):
    """
    Establish a connection to the database.
    """
    return pymysql.connect(
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"],
        port=db_config["port"],
        connect_timeout=10,
        cursorclass=pymysql.cursors.DictCursor
    )
