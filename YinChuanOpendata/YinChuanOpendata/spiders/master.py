# -*- coding: utf-8 -*-
import scrapy
import json
from scrapy import Request, FormRequest
from YinChuanOpendata.items import YinchuanopendataItem
from ..utils.LastCrawl import LastCrawl
from ..utils import toolkit, unify_url


class MasterSpider(scrapy.Spider):
    name = 'master'
    start_url = 'http://data.yinchuan.gov.cn/odweb/catalog/catalog.do?method=GetCatalog'
    last_crawl_time = LastCrawl.crawl_time()

    def start_requests(self):
        data = {
            'start': '0',
            'length': '6',
            'pageLength': '6',
            '_order': '1:b'
        }
        yield FormRequest(url=self.start_url, formdata=data, callback=self.dataset_list_parse, meta={'data': data})

    def dataset_list_parse(self, response):
        dataset_crawled = 0
        res = json.loads(response.text)
        page_parameter = response.meta['data']
        start_num = page_parameter['start']
        file_url = 'http://data.yinchuan.gov.cn/odweb/catalog/catalogDetail.do?method=GetDownLoadInfo'
        for data in res['data']:
            update_time = toolkit.date_to_stamp(data['update_time'].split(' ')[0])
            if update_time < self.last_crawl_time:  # 如果更新时间小于上次爬取时间，就跳过
                dataset_crawled += 1
                continue
            metadata = dict()
            metadata['数据目录名称'] = data['description']
            metadata['开放状态'] = data['open_type']
            metadata['标签'] = data['cata_tags']
            metadata['来源部门'] = data['org_name']
            metadata['更新时间'] = data['update_time']
            metadata['发布时间'] = data['released_time']
            data_id = data['cata_id']
            cata_id = {
                'cata_id': str(data_id)
            }
            yield FormRequest(url=file_url,
                              formdata=cata_id,
                              callback=self.file_parse,
                              meta={'metadata': metadata,
                                    'cata_id': str(data_id)
                                    }
                              )

        if int(res['recordsTotal']) - int(start_num) <= 5 or dataset_crawled >= 6:
            return
        else:
            num = int(start_num) + 6
            form_data = {
                'start': str(num),
                'length': '6',
                'pageLength': '6',
                '_order': '1:b'
            }
            yield FormRequest(url=self.start_url, formdata=form_data, callback=self.dataset_list_parse, meta={'data': form_data})

    def file_parse(self, response):
        host = 'http://data.yinchuan.gov.cn/odweb/catalog/catalogDetail.htm?cata_id='
        file_data = dict()
        file_list = json.loads(response.text)['data']
        file_data['metadata'] = response.meta['metadata']
        for file in file_list:
            file_data['file_type'] = file['fileFormat']
            file_data['file_name'] = file['fileName']
            file_data['file_id'] = file['fileId']
            yield Request(url=host+response.meta['cata_id'], callback=self.datafield_parse, meta={'item': file_data})

    def datafield_parse(self, response):
        item = YinchuanopendataItem()
        datafield = list()
        item['metadata'] = response.meta['item']['metadata']
        item['file_name'] = response.meta['item']['file_name']
        item['file_id'] = response.meta['item']['file_id']
        item['file_type'] = response.meta['item']['file_type']
        datafield_list = response.xpath('//div[@class="detail-table"]//tr')
        for index, field_row in enumerate(datafield_list):
            row = list()
            if index == 0:
                for one in field_row.xpath('th'):
                    row.append(one.xpath('text()').extract_first())
                datafield.append(row)
            else:
                for one in field_row.xpath('td'):
                    row.append(one.xpath('text()').extract_first())
                datafield.append(row)
        item['datafield'] = datafield
        yield item

