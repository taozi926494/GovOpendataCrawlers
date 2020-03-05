# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class YinchuanopendataItem(scrapy.Item):
    metadata = scrapy.Field()
    file_name = scrapy.Field()
    file_id = scrapy.Field()
    datafield = scrapy.Field()
    file_type = scrapy.Field()
