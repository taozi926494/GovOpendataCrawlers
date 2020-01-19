# -*- coding: utf-8 -*-
import scrapy


class FileItem(scrapy.Item):
    file_urls = scrapy.Field()
    title = scrapy.Field()
    file_names = scrapy.Field()
    metadata = scrapy.Field()
