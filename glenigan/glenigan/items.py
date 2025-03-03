# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class ApplicationItem(scrapy.Item):
    """Defines the structure of the scraped data."""
    ref_no = scrapy.Field()
    link = scrapy.Field()

class HtmlScraperItem(scrapy.Item):
    ref_no = scrapy.Field()
    url = scrapy.Field()
    html_content = scrapy.Field()

    def __repr__(self):
        """Avoid logging large HTML content"""
        return f"HtmlScraperItem(ref_no={self.get('ref_no')}, url={self.get('url')}, html_content=<hidden>)"
