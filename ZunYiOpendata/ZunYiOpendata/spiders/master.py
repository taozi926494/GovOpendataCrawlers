# -*- coding: utf-8 -*-
import scrapy
import json
import requests
from scrapy import FormRequest, Request
from ..items import ZunyiopendataItem
from ..utils.LastCrawl import LastCrawl
from ..utils import toolkit, unify_url

class MasterSpider(scrapy.Spider):
    name = 'master'
    url = 'http://www.zyopendata.gov.cn/dataopen/api/dataset?callback=jQuery&pageNo='
    last_crawl_time = LastCrawl.crawl_time()

    def start_requests(self):
        pageNo = 1
        start_url = self.url + str(pageNo)
        yield Request(url=start_url, callback=self.dataset_list_parse, meta={'pageNo': pageNo, 'crawl': 0})

    def dataset_list_parse(self, response):
        dataset_crawled = 0
        crawl = response.meta['crawl']
        next_page = response.meta['pageNo'] + 1
        info_host = 'http://www.zyopendata.gov.cn/dataopen/api/filedata/'
        info_end = '?callback=jQuery'
        dataset_list = json.loads(response.text[11: -2])['data']['datasetlist']
        all_dataset = json.loads(response.text[11: -2])['data']['totalCount']
        for dataset in dataset_list:
            dataset_update_time = toolkit.date_to_stamp(dataset['updTime'].split(' ')[0])
            if dataset_update_time < self.last_crawl_time:  # 如果发布时间小于上次爬取时间，就跳过
                dataset_crawled += 1
                continue
            metadata = dict()
            metadata['数据摘要'] = dataset['name']
            metadata['主题名称'] = dataset['topicName']
            metadata['数据提供方'] = dataset['orgName']
            metadata['最后更新时间'] = dataset['updTime']
            id_list = dataset['list']
            for i in id_list:
                info_url = info_host + str(i['id']) + info_end
                yield Request(url=info_url, callback=self.dataset_file_parse, meta={'metadata': metadata})
        if crawl < all_dataset and dataset_crawled >= 10:
            next_url = self.url + str(next_page)
            yield Request(url=next_url, callback=self.dataset_list_parse, meta={'pageNo': next_page, 'crawl': crawl})

    def dataset_file_parse(self, response):
        item = ZunyiopendataItem()
        res = json.loads(response.text[11: -2])['data']
        fileInfo_url = 'http://www.zyopendata.gov.cn/dataopen/api/url/' + res['shortUrl'] + '?callback=jQuery'
        file_name = res['remark']
        annex_url = json.loads(requests.get(url=fileInfo_url).text[11: -2])['data']['realUrl']
        item['file_name'] = file_name
        item['annex_url'] = annex_url
        item['metadata'] = response.meta['metadata']
        yield item


