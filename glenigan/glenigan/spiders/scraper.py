import scrapy
from scrapy.http import FormRequest
import pymysql
import configparser
import os
import re
import json
from glenigan.items import ApplicationItem, HtmlScraperItem
from glenigan.logger_config import logger

class ScraperSpider(scrapy.Spider):
    name = "scraper"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.check_updates = kwargs.get("check_updates", "no")
        self.crawler_type = kwargs.get("crawler_type", "planning")

        # Load council details
        json_path = r"C:\Users\naafiah.fathima\Desktop\glenigan_scrapy1\glenigan\glenigan\councils.json"
        if not os.path.exists(json_path):
            raise FileNotFoundError(f"Councils JSON file not found at: {json_path}")
        with open(json_path, "r") as file:
            self.councils = json.load(file)

        # Load database configuration
        config_path = r"C:\Users\naafiah.fathima\Desktop\glenigan_scrapy1\glenigan\glenigan\database.ini"
        self.db_config = self.load_db_config(config_path)
        
        # Define tabs to scrape
        self.tabs = ["summary", "details", "contacts", "dates", "makeComment", "neighbourComments", "consulteeComments", "constraints", "documents", "relatedCases"]

    def load_db_config(self, path):
        config = configparser.ConfigParser()
        config.read(path)
        return {
            "host": config["mysql"]["host"],
            "user": config["mysql"]["user"],
            "password": config["mysql"]["password"],
            "database": config["mysql"]["database"],
            "port": int(config["mysql"]["port"])
        }

    def start_requests(self):
        """Start scraping only for applications that have not been scraped."""
        connection = pymysql.connect(**self.db_config)
        cursor = connection.cursor()

        for council_name, council_info in self.councils.items():
            # Fetch existing applications and their scrape_status
            cursor.execute(f"SELECT ref_no, scrape_status FROM {self.get_app_table()} WHERE ref_no LIKE %s", (f"{council_info['code']}_%",))
            existing_records = {row[0]: row[1] for row in cursor.fetchall()}

            yield scrapy.Request(
                url=council_info["url"],
                callback=self.parse,
                meta={"council_name": council_name, "council_code": council_info["code"], "url": council_info["url"], "existing_records": existing_records},
            )

        cursor.close()
        connection.close()

    def parse(self, response):
        """Extract CSRF token and submit form request."""
        csrf_token = response.xpath('//form[@id="advancedSearchForm"]//input[@name="_csrf"]/@value').get()
        if not csrf_token:
            return

        if self.crawler_type == "decision":
            form_data = {
                "_csrf": csrf_token,
                "date(applicationDecisionStart)": "18/02/2025",
                "date(applicationDecisionEnd)": "20/02/2025",
                "searchType": "Application",
            }
        else:  # planning
            form_data = {
                "_csrf": csrf_token,
                "date(applicationValidatedStart)": "19/02/2025",
                "date(applicationValidatedEnd)": "19/02/2025",
                "searchType": "Application",
            }
        post_url = response.meta["url"].replace("search.do?action=advanced", "advancedSearchResults.do")
        yield FormRequest(
            url=f"{post_url}?action=firstPage",
            formdata=form_data,
            callback=self.parse_results,
            meta=response.meta,
            method="POST",
        )

    def get_app_table(self):
        """Return the table name for application data based on crawler_type."""
        return "decision_app" if self.crawler_type == "decision" else "plan_app"

    def get_error_table(self):
        """Return the table name for errors based on crawler_type."""
        return "decision_error" if self.crawler_type == "decision" else "plan_error"

    def parse_results(self, response):
        """Extract application details and immediately start HTML scraping."""
        applications = response.xpath('//li[contains(@class, "searchresult")]')
        if not applications:
            return
        
        existing_records = response.meta["existing_records"]

        for app in applications:
            link_tag = app.xpath(".//a")
            link = response.meta["url"].split("/online-applications")[0] + link_tag.xpath("./@href").get() if link_tag else "N/A"
            ref_no = app.xpath('.//p[@class="metaInfo"]/text()').re_first(r"Ref\. No:\s*([\w/.-]+)")
            sanitized_ref_no = self.sanitize_ref_no(f"{response.meta['council_code']}_{ref_no}")

            # Determine if we should scrape this application and whether it's a rescrape
            if sanitized_ref_no in existing_records:
                current_status = existing_records[sanitized_ref_no]
                if current_status == "Yes(R)":
                    logger.info(f"Skipping already scraped application with Yes(R): {sanitized_ref_no}")
                    continue  # Do not overwrite Yes(R)
                elif current_status == "Yes":
                    if self.check_updates.lower() == "yes":
                        logger.info(f"Rescraping application (check_updates=yes): {sanitized_ref_no}")
                        rescrape = True
                    else:
                        logger.info(f"Skipping already scraped application: {sanitized_ref_no}")
                        continue
                elif current_status == "No":
                    logger.info(f"Scraping application with status No: {sanitized_ref_no}")
                    rescrape = False
            else:
                # logger.info(f"Inserting new application: {sanitized_ref_no}")
                # self.insert_new_application(sanitized_ref_no, link)
                rescrape = False

            # Pass the is_rescrape flag with the item and in meta for downstream use
            yield ApplicationItem(ref_no=sanitized_ref_no, link=link, is_rescrape=rescrape)
            yield scrapy.Request(
                url=link,
                callback=self.parse_html,
                meta={
                    "ref_no": sanitized_ref_no,
                    "base_url": link,
                    "all_html_content": "",
                    "tab_index": 0,
                    "rescrape": rescrape
                },
                dont_filter=True
            )

        # Handle pagination as before
        next_page_tag = response.xpath('//a[contains(@class, "next")]/@href').get()
        if next_page_tag:
            next_page_url = response.meta["url"].split("/online-applications")[0] + next_page_tag
            yield scrapy.Request(url=next_page_url, callback=self.parse_results, meta=response.meta)

    def insert_new_application(self, ref_no, url):
        """Insert new application into the appropriate table with scrape_status = 'No'."""
        connection = pymysql.connect(**self.db_config)
        cursor = connection.cursor()
        table_name = self.get_app_table()
        try:
            cursor.execute(
                f"INSERT INTO {table_name} (ref_no, Url, scrape_status) VALUES (%s, %s, 'No')",
                (ref_no, url)
            )
            connection.commit()
        except Exception as e:
            logger.error(f"Error inserting application {ref_no}: {e}")
        finally:
            cursor.close()
            connection.close()

    def parse_html(self, response):
        """Extracts main HTML and starts scraping tabs."""
        ref_no = response.meta['ref_no']
        base_url = response.meta['base_url']
        all_html_content = f"\n<!-- Main Page -->\n{response.text}"
        
        yield scrapy.Request(
            url=self.construct_tab_url(base_url, self.tabs[0]),
            callback=self.parse_tab,
            meta={
                "ref_no": ref_no,
                "all_html_content": all_html_content,
                "tab_index": 0,
                "base_url": base_url,
                "rescrape": response.meta.get("rescrape", False)
            },
            dont_filter=True
        )

    def parse_tab(self, response):
        """Extract each tab's content and save once all tabs are scraped."""
        try:
            ref_no = response.meta["ref_no"]
            all_html_content = response.meta["all_html_content"]
            tab_index = response.meta["tab_index"]
            base_url = response.meta["base_url"]

            tab_name = self.tabs[tab_index]
            all_html_content += f"\n<!-- Tab: {tab_name} -->\n{response.text}"

            logger.info(f"Successfully scraped tab {tab_name} for {ref_no}")

            next_tab_index = tab_index + 1
            if next_tab_index < len(self.tabs):
                yield scrapy.Request(
                    url=self.construct_tab_url(base_url, self.tabs[next_tab_index]),
                    callback=self.parse_tab,
                    meta={
                        "ref_no": ref_no,
                        "all_html_content": all_html_content,
                        "tab_index": next_tab_index,
                        "base_url": base_url,
                        "rescrape": response.meta.get("rescrape", False)
                    },
                    errback=self.handle_tab_error,
                    dont_filter=True
                )
            else:
                yield HtmlScraperItem(
                    ref_no=ref_no,
                    url=base_url,
                    html_content=all_html_content,
                    is_rescrape=response.meta.get("rescrape", False)
                )
        except Exception as e:
            self.log_error(ref_no, f"Failed to scrape tab: {self.tabs[tab_index]}, Error: {str(e)}")
            logger.error(f"Failed to scrape tab {self.tabs[tab_index]} for {ref_no}: {e}")

    def handle_tab_error(self, failure):
        """Handles errors when a tab scraping request fails."""
        request = failure.request
        ref_no = request.meta.get("ref_no", "Unknown")
        tab_index = request.meta.get("tab_index", -1)
        tab_name = self.tabs[tab_index] if tab_index >= 0 else "Unknown"
        error_msg = repr(failure.value)

        logger.error(f"Tab scraping failed for {ref_no}, Tab: {tab_name}, Error: {error_msg}")

        # Save error in the database
        self.log_error(ref_no, f"Failed to scrape tab {tab_name}: {error_msg}")
        
    def log_error(self, ref_no, error_msg):
        """Logs errors into the appropriate table."""
        table_name = self.get_error_table()
        try:
            connection = pymysql.connect(**self.db_config)
            cursor = connection.cursor()
            cursor.execute(
                f"INSERT INTO {table_name} (ref_no, error) VALUES (%s, %s) ON DUPLICATE KEY UPDATE error = %s",
                (ref_no, error_msg, error_msg)
            )
            connection.commit()
            logger.info(f"Error logged for {ref_no}: {error_msg}")
            cursor.close()
            connection.close()
        except Exception as e:
            logger.error(f"Database logging failed for {ref_no}: {str(e)}")

    def construct_tab_url(self, base_url, tab_name):
        """Constructs the correct tab URL."""
        if "activeTab=" in base_url:
            return base_url.split("activeTab=")[0] + f"activeTab={tab_name}"
        else:
            return base_url + f"&activeTab={tab_name}"
    def sanitize_ref_no(self, ref_no):
        """Sanitize reference numbers."""
        return re.sub(r'[^a-zA-Z0-9_-]', '_', ref_no)
