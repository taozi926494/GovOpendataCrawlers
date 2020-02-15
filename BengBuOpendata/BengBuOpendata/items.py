# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BengbuopendataItem(scrapy.Item):
    file_name = scrapy.Field()
    metadata = scrapy.Field()
    file_id = scrapy.Field()