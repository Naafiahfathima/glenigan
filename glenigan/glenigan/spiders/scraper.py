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
        """Start scraping for each council from JSON file."""
        for council_name, council_info in self.councils.items():
            yield scrapy.Request(
                url=council_info["url"],
                callback=self.parse,
                meta={"council_name": council_name, "council_code": council_info["code"], "url": council_info["url"]},
            )

    def parse(self, response):
        """Extract CSRF token and submit form request."""
        csrf_token = response.xpath('//form[@id="advancedSearchForm"]//input[@name="_csrf"]/@value').get()
        if not csrf_token:
            return

        form_data = {
            "_csrf": csrf_token,
            "date(applicationValidatedStart)": "18/02/2025",
            "date(applicationValidatedEnd)": "18/02/2025",
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

    def parse_results(self, response):
        """Extract application details and immediately start HTML scraping."""
        applications = response.xpath('//li[contains(@class, "searchresult")]')
        if not applications:
            return

        for app in applications:
            link_tag = app.xpath(".//a")
            link = response.meta["url"].split("/online-applications")[0] + link_tag.xpath("./@href").get() if link_tag else "N/A"
            ref_no = app.xpath('.//p[@class="metaInfo"]/text()').re_first(r"Ref\. No:\s*([\w/.-]+)")
            sanitized_ref_no = self.sanitize_ref_no(f"{response.meta['council_code']}_{ref_no}")

            # Scrape and immediately fetch HTML dump
            yield ApplicationItem(ref_no=sanitized_ref_no, link=link)
            yield scrapy.Request(
                url=link,
                callback=self.parse_html,
                meta={"ref_no": sanitized_ref_no, "base_url": link, "all_html_content": "", "tab_index": 0},
                dont_filter=True
            )

        # Handle pagination
        next_page_tag = response.xpath('//a[contains(@class, "next")]/@href').get()
        if next_page_tag:
            next_page_url = response.meta["url"].split("/online-applications")[0] + next_page_tag
            yield scrapy.Request(url=next_page_url, callback=self.parse_results, meta=response.meta)

    def parse_html(self, response):
        """Extracts main HTML and starts scraping tabs."""
        ref_no = response.meta['ref_no']
        base_url = response.meta['base_url']
        all_html_content = f"\n<!-- Main Page -->\n{response.text}"
        
        yield scrapy.Request(
            url=self.construct_tab_url(base_url, self.tabs[0]),
            callback=self.parse_tab,
            meta={"ref_no": ref_no, "all_html_content": all_html_content, "tab_index": 0, "base_url": base_url},
            dont_filter=True
        )

    def parse_tab(self, response):
        """Extract each tab's content and save once all tabs are scraped."""
        ref_no = response.meta["ref_no"]
        all_html_content = response.meta["all_html_content"]
        tab_index = response.meta["tab_index"]
        base_url = response.meta["base_url"]

        tab_name = self.tabs[tab_index]
        all_html_content += f"\n<!-- Tab: {tab_name} -->\n{response.text}"

        next_tab_index = tab_index + 1
        if next_tab_index < len(self.tabs):
            yield scrapy.Request(
                url=self.construct_tab_url(base_url, self.tabs[next_tab_index]),
                callback=self.parse_tab,
                meta={"ref_no": ref_no, "all_html_content": all_html_content, "tab_index": next_tab_index, "base_url": base_url},
                dont_filter=True
            )
        else:
            yield HtmlScraperItem(ref_no=ref_no, url=base_url, html_content=all_html_content)

    def construct_tab_url(self, base_url, tab_name):
        """Constructs the correct tab URL."""
        if "activeTab=" in base_url:
            return base_url.split("activeTab=")[0] + f"activeTab={tab_name}"
        else:
            return base_url + f"&activeTab={tab_name}"
    def sanitize_ref_no(self, ref_no):
        """Sanitize reference numbers."""
        return re.sub(r'[^a-zA-Z0-9_-]', '_', ref_no)
