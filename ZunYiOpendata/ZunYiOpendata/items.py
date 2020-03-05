# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ZunyiopendataItem(scrapy.Item):
    file_name = scrapy.Field()
    annex_url = scrapy.Field()
    metadata = scrapy.Field()