# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ChengduopendataItem(scrapy.Item):
    fileId = scrapy.Field()
    fileName = scrapy.Field()
    metadata = scrapy.Field()
    datafield = scrapy.Field()
