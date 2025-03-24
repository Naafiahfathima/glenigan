# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import configparser
import logging
import os
import re

import pymysql
from glenigan.items import ApplicationItem, HtmlScraperItem
from glenigan.logger_config import logger
from scrapy.exceptions import DropItem
from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_exponential)


class GleniganPipeline:
    def __init__(self):
        self.output_folder = "html_dumps"
        self.db_config = self.load_db_config()

        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

    def load_db_config(self):
        config = configparser.ConfigParser()
        config.read(
            r"C:\Users\naafiah.fathima\Desktop\glenigan_scrapy\glenigan\glenigan\database.ini")
        return {
            "host": config["mysql"]["host"],
            "user": config["mysql"]["user"],
            "password": config["mysql"]["password"],
            "database": config["mysql"]["database"],
            "port": int(config["mysql"]["port"])
        }

    def open_spider(self, spider):
        """Connects to the database when the spider starts."""
        self.conn = pymysql.connect(**self.db_config)
        self.cursor = self.conn.cursor()

        # Get table names from the spider based on crawler_type
        self.app_table = spider.get_app_table()
        self.error_table = spider.get_error_table()

        if self.app_table == "decision_app":
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.app_table} (
                    ref_no VARCHAR(255) PRIMARY KEY,
                    Url TEXT,
                    scrape_status VARCHAR(10) DEFAULT 'No',
                    rescrape_status VARCHAR(10) DEFAULT 'No'
                )
            """)
        else:
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.app_table} (
                    ref_no VARCHAR(255) PRIMARY KEY,
                    Url TEXT,
                    scrape_status VARCHAR(10) DEFAULT 'No'
                )
            """)
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.error_table} (
                ref_no VARCHAR(255),
                error TEXT,
                PRIMARY KEY (ref_no)
            )
        """)

    def process_item(self, item, spider):
        """Process items based on their type."""
        if isinstance(item, ApplicationItem):
            self.process_application_item(item)
        elif isinstance(item, HtmlScraperItem):
            self.process_html_scraper_item(item)
        return item

    def clean_html_content(self, html):
        """Clean HTML by removing comments, script tags, and extra whitespace."""
        # Remove HTML comments (including multi-line comments)
        html = re.sub(r'<!--[\s\S]*?-->', '', html)
        # Remove all script tags and their content
        html = re.sub(r'<script[\s\S]*?>[\s\S]*?</script>',
                      '', html, flags=re.IGNORECASE)
        # Remove all closing tags (e.g., </p>, </div>, etc.)
        html = re.sub(r'</[^>]+>', '', html)
        # Split content into lines, strip whitespace, and remove empty lines
        lines = [line.strip() for line in html.splitlines() if line.strip()]
        return "\n".join(lines)

    def process_application_item(self, item):
        """Inserts application data into the appropriate table."""
        ref_no = item["ref_no"]
        url = item["link"]

        self.cursor.execute(
            f"SELECT * FROM {self.app_table} WHERE ref_no = %s", (ref_no,))
        if self.cursor.fetchone():
            raise DropItem(f"Duplicate entry: {ref_no}")

        try:
            self.cursor.execute(
                f"INSERT INTO {self.app_table} (ref_no, Url) VALUES (%s, %s)", (ref_no, url))
            self.conn.commit()
            logger.info(f"Inserted Application: {ref_no}")
        except Exception as e:
            logger.error(f"Unexpected error processing {ref_no}: {e}")
            raise DropItem(f"Unexpected error processing {ref_no}: {e}")

    def process_html_scraper_item(self, item):
        """Process HTML scraper item and update scrape status immediately."""
        ref_no = item['ref_no']
        html_content = item['html_content']

        # Clean the HTML dump before storing it
        html_content = self.clean_html_content(html_content)

        sanitized_ref_no = ref_no.replace("/", "_")
        filename = os.path.join(self.output_folder, f"{sanitized_ref_no}.html")
        with open(filename, "w", encoding="utf-8") as file:
            file.write(html_content)

        logger.info(f"Saved: {filename}")
        is_rescrape = item.get("is_rescrape", False)
        self.update_scrape_status(ref_no, is_rescrape)

    @retry(
        retry=retry_if_exception_type(pymysql.MySQLError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=1, max=10),
        reraise=True,
    )
    def update_scrape_status(self, ref_no, is_rescrape=False):
        """Update status in the database: update rescrape_status if this is a rescrape,
        otherwise update scrape_status."""
        try:
            if is_rescrape:
                # For rescraped applications, update only rescrape_status
                self.cursor.execute(
                    f"UPDATE {self.app_table} SET rescrape_status = 'yes' WHERE ref_no = %s", (
                        ref_no,)
                )
                logger.info(f"Updated rescrape_status to 'yes' for {ref_no}")
            else:
                # For new applications, update the normal scrape_status
                self.cursor.execute(
                    f"UPDATE {self.app_table} SET scrape_status = 'Yes' WHERE ref_no = %s", (
                        ref_no,)
                )
                logger.info(f"Updated scrape_status to 'Yes' for {ref_no}")
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error updating status for {ref_no}: {e}")

    def close_spider(self, spider):
        """Closes the database connection when the spider finishes."""
        self.cursor.close()
        self.conn.close()
