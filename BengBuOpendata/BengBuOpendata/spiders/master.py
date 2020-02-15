# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request, FormRequest
import urllib.parse
from BengBuOpendata.items import BengbuopendataItem
from ..utils.LastCrawl import LastCrawl

class MasterSpider(scrapy.Spider):
    name = 'master'
    Host = 'http://data.bengbu.gov.cn/cn/data/list_26.aspx?'
    last_crawl_time = LastCrawl.crawl_time()

    def start_requests(self):
        data = {
            'page': 1,
            'sort': 2,
            'departCategory': 0,
            'themeCategory': 0
        }
        url = self.Host + urllib.parse.urlencode(data)
        yield Request(url=url, callback=self.dataset_list_parse, meta={'page_data': data})

    def dataset_list_parse(self, response):
        host = 'http://data.bengbu.gov.cn'
        res = response.xpath('//ul[@class="SJXZ-UL"]//li//a[@class="SJXZ-Name"]/@href').extract()
        for dataset_url in res:
            url = host + dataset_url
            yield Request(url=url, callback=self.dataset_parse)
        page_num = response.xpath('//div[@class="Pages"]//span[@class="p_count"]//em[1]/text()').extract()[0]
        if response.meta['page_data']['page'] < int(page_num):
            print('############################')
            page_data = {
            'page': response.meta['page_data']['page'] + 1,
            'sort': 2,
            'departCategory': 0,
            'themeCategory': 0
        }
            yield Request(url=self.Host + urllib.parse.urlencode(page_data),
                          callback=self.dataset_list_parse,
                          meta={'page_data': page_data})

    def dataset_parse(self, response):
        item = BengbuopendataItem()
        metadata = dict()
        message = response.xpath('//ul[@class="XZXQ-UL clearfix"]//li/text()').extract()
        metadata['名称'] = response.xpath('//div[@class="XZXQ-Name"]/h3/text()').extract()[0]
        metadata['数据主题'] = message[1].split(' ')[-1]
        metadata['数据提供方'] = message[2].split(' ')[-1]
        metadata['关键字'] = message[3].split(' ')[-1]
        metadata['发布时间'] = message[4].split(' ')[-1]
        metadata['更新时间'] = message[5].split(' ')[-1]
        item['metadata'] = metadata
        item['file_name'] = response.xpath('normalize-space(//table[@class="XZLB-Box"]//tr[3]//a/text())').extract()[0]
        file_Info = response.xpath('//table[@class="XZLB-Box"]//tr[3]//a/@href').extract()[0][-8: -2]
        file_data = {
            'did': int(file_Info.split(',')[0]),
            'nid': int(file_Info.split(',')[1])
        }
        item['file_id'] = file_data
        yield item



