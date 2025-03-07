# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os
import pymysql
import logging
from scrapy.exceptions import DropItem
import configparser
from glenigan.logger_config import logger
from glenigan.items import ApplicationItem, HtmlScraperItem
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class GleniganPipeline:
    def __init__(self):
        self.output_folder = "html_dumps"
        self.db_config = self.load_db_config()
        
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

    def load_db_config(self):
        config = configparser.ConfigParser()
        config.read(r"C:\Users\naafiah.fathima\Desktop\glenigan_scrapy\glenigan\glenigan\database.ini")
        return {
            "host": config["mysql"]["host"],
            "user": config["mysql"]["user"],
            "password": config["mysql"]["password"],
            "database": config["mysql"]["database"],
            "port": int(config["mysql"]["port"])
        }

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.db_config)
        self.cursor = self.conn.cursor()
        # Determine crawler_type from the spider (default to "planning")
        crawler_type = getattr(spider, "crawler_type", "planning")
        if crawler_type == "decision":
            self.table_app = "decision_app"
            self.table_err = "decision_error"
        else:
            self.table_app = "plan_app"
            self.table_err = "plan_error"
        # Create dynamic tables based on crawler_type
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_app} (
                ref_no VARCHAR(255) PRIMARY KEY,
                Url TEXT,
                scrape_status VARCHAR(10) DEFAULT 'No'
            )
        """)
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_err} (
                ref_no VARCHAR(255),
                error TEXT,
                PRIMARY KEY (ref_no)
            )
        """)
        # Capture the check_updates flag from the spider
        self.check_updates = getattr(spider, "check_updates", "no")

    def process_item(self, item, spider):
        """Process items based on their type."""
        if isinstance(item, ApplicationItem):
            self.process_application_item(item)
        elif isinstance(item, HtmlScraperItem):
            self.process_html_scraper_item(item)
        return item

    def process_application_item(self, item):
        """Inserts application data into the dynamic table."""
        ref_no = item["ref_no"]
        url = item["link"]
        self.cursor.execute(f"SELECT * FROM {self.table_app} WHERE ref_no = %s", (ref_no,))
        if self.cursor.fetchone():
            raise DropItem(f"Duplicate entry: {ref_no}")

        try:
            self.cursor.execute(f"INSERT INTO {self.table_app} (ref_no, Url) VALUES (%s, %s)", (ref_no, url))
            self.conn.commit()
            logger.info(f"Inserted Application: {ref_no}")
        except Exception as e:
            logger.error(f"Unexpected error processing {ref_no}: {e}")
            raise DropItem(f"Unexpected error processing {ref_no}: {e}")
    
    

    def process_html_scraper_item(self, item):
        """Process HTML scraper item and update scrape status immediately."""
        ref_no = item['ref_no']
        html_content = item['html_content']
        sanitized_ref_no = ref_no.replace("/", "_")
        filename = os.path.join(self.output_folder, f"{sanitized_ref_no}.html")
        with open(filename, "w", encoding="utf-8") as file:
            file.write(html_content)
        logger.info(f"Saved: {filename}")
        self.update_scrape_status(ref_no, item.get("is_rescrape", False))

    @retry(
        retry=retry_if_exception_type(pymysql.MySQLError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=1, max=10),
        reraise=True,
    )
    def update_scrape_status(self, ref_no, is_rescrape=False):
        new_status = "Yes(R)" if is_rescrape else "Yes"
        self.cursor.execute(f"SELECT scrape_status FROM {self.table_app} WHERE ref_no = %s", (ref_no,))
        result = self.cursor.fetchone()
        if result:
            current_status = result[0]
            if current_status == "Yes(R)":
                logger.info(f"Status is already Yes(R) for {ref_no}. Not updating.")
                return
        try:
            self.cursor.execute(f"UPDATE {self.table_app} SET scrape_status = %s WHERE ref_no = %s", (new_status, ref_no))
            self.conn.commit()
            logger.info(f"Updated scrape_status to '{new_status}' for {ref_no}")
        except Exception as e:
            logger.error(f"Error updating scrape_status for {ref_no}: {e}")

    def close_spider(self, spider):
        """Closes the database connection when the spider finishes."""
        self.cursor.close()
        self.conn.close()
