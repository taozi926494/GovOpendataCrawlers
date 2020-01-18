# -*- coding: utf-8 -*-
import json
import re
from urllib.parse import urljoin

import scrapy
from scrapy import Request, FormRequest
from ..items import FileDownloadItem
from ..utils.LastCrawl import LastCrawl
from ..utils.FileSummary import FilesSummary
from ..utils import toolkit


class MasterSpider(scrapy.Spider):
    name = 'master'
    last_crawl_time = 0

    def start_requests(self) -> 'Request':
        self.last_crawl_time = LastCrawl.crawl_time()
        url = 'http://www.gyopendata.gov.cn/city/datadirectory.htm?type=0&value1=0&pageNo=1'
        yield Request(url=url, callback=self.parse_list)

    def parse_list(self, response):
        dataset_crawled = 0

        datasets = response.xpath('*//div[@class="t1-sub-box-list"]')
        for dataset in datasets:
            dataset_update_time = dataset.xpath('div[@class="gxsj"]/span/text()').extract_first()
            dataset_update_time = toolkit.date_to_stamp(dataset_update_time)
            # 如果更新时间大于上次爬取的时间 则下载数据集
            if dataset_update_time > self.last_crawl_time:
                dataset_url = dataset.xpath('div[@class="name"]/a/@href').extract_first()
                # 相对路径转绝对路径
                dataset_url = urljoin(response.url, dataset_url)
                yield Request(url=dataset_url, callback=self.parse_dataset)
            else:
                dataset_crawled += 1

        # 每页5条数据，如果有超过5条数据被爬过，就不再翻页
        if dataset_crawled >= 5:
            return
        else:
            # <a href="javascript:void(0);" onclick="javascript:window.location.href='datadirectory.htm?type=2&amp;value1=0&amp;pageNo=2'"><font size="2">下一页</font></a>
            next_page = response.xpath('*//div[@class="page"]/span/a[contains(string(), "下一页")]/@onclick').extract_first()
            if next_page:
                next_url = re.search('\'(.*?)\'', next_page).group(1)
                # 相对路径转绝对路径
                next_url = urljoin(response.url, next_url)
                yield Request(url=next_url, callback=self.parse_list)

    def parse_dataset(self, response) -> 'FormRequest':
        metadata_tds = response.xpath('*//table[contains(string(), "摘要") and contains(string(), "数据领域")]//td/span')
        metadata = {}
        for meta_index, meta_td in enumerate(metadata_tds):
            # 偶数位的值作为key 奇数位的值作为value
            if meta_index % 2 == 0:
                meta_key = meta_td.xpath('text()').extract_first()
                meta_key = toolkit.remove_blanks(meta_key)

                next_meta = metadata_tds[meta_index + 1]
                meta_val = next_meta.xpath('text()').extract_first()
                if meta_val is not None:
                    meta_val = toolkit.remove_blanks(meta_val)

                metadata[meta_key] = meta_val

        dataset_id = re.search('resId=(.*)', response.url).group(1)

        yield FormRequest(url='http://www.gyopendata.gov.cn/city/filelist.htm', formdata={
            "resId": dataset_id,
            "text1": "all",
            "doId": "2"
        }, callback=self.parse_filelist, meta=metadata)

    def parse_filelist(self, response) -> 'FileDownloadItem':
        file_list = json.loads(response.text)
        uploadNames = []
        uploadDirs = []
        for file in file_list:
            uploadNames.append(file["uploadName"])
            uploadDirs.append(file["uploadDir"])

        url = 'http://www.gyopendata.gov.cn/city/downloadFileZip.htm'
        metadata = response.meta
        formdata = {
            "resId": metadata['标识符'],
            "zipName": metadata['名称'],
            "uploadNames": ','.join(uploadNames),
            "uploadDirs": ','.join(uploadDirs)
        }

        file_download_item = FileDownloadItem()
        file_download_item['file_urls'] = [url]
        file_download_item['formdata'] = formdata
        file_download_item['metadata'] = response.meta

        # yield 文件下载的item，到FilePipeline中去下载文件
        yield file_download_item

