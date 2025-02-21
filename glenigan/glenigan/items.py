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
