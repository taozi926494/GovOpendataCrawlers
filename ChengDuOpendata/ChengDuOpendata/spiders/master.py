# -*- coding: utf-8 -*-
import scrapy
import json
from scrapy import FormRequest, Request
from ..items import ChengduopendataItem
from ..utils.LastCrawl import LastCrawl


class MasterSpider(scrapy.Spider):
    name = 'master'
    last_crawl_time = LastCrawl.crawl_time()
    data_list_url = 'http://www.cddata.gov.cn/odweb/catalog/catalog.do?method=GetCatalog'

    def start_requests(self):
        start_num = 0
        form_data = {
            'start': '0',
            'length': '6',
            'pageLength': '6',
            'cata_type': 'default',
            '_order': '1:b'
        }
        yield FormRequest(url=self.data_list_url, formdata=form_data,
                          callback=self.metadata_parse, meta={'start_num': start_num})

    def metadata_parse(self, response):
        dataset_crawled = 0
        Host = 'http://www.cddata.gov.cn/odweb/catalog/CatalogDetail.do?method=getDownLoadPageInfo&cata_id='
        filetype_list = ['&file_typs=1&keywords=csv', '&file_typs=1&keywords=json', '&file_typs=1&keywords=xls',
                         '&file_typs=1&keywords=xml', '&file_typs=3']
        form_data = {
            'draw': '1',
            'start': '0',
            'length': '10',
            'search[regex]': 'false'
        }
        data_num = response.meta['start_num']
        response_data = json.loads(response.text)
        for data in response_data['data']:
            update_str = str(data['conf_update_time'])
            update_time = int(update_str[0:10])
            if update_time < self.last_crawl_time:  # 如果更新时间小于上次爬取时间，就跳过
                dataset_crawled += 1
                continue
            metadata = dict()
            metadata['数据目录名称'] = data['cata_title']
            metadata['开放状态'] = data['open_type']
            metadata['最后更新'] = data['update_time']
            metadata['来源部门'] = data['org_name']
            metadata['标签'] = data['cata_items']
            metadata['描述'] = data['description']
            cata_id = data['cata_id']
            for file_type in filetype_list:
                url = Host + cata_id + file_type
                yield FormRequest(url=url, formdata=form_data,
                                  callback=self.file_parse,
                                  meta={'metadata': metadata,
                                        'cata_id': str(data['cata_id'])
                                        })

        if response_data['recordsTotal'] - data_num <= 5 or dataset_crawled >= 6:
            return
        else:
            data_num += 6
            next_form_data = {
                'start': str(data_num),
                'length': '6',
                'pageLength': '6',
                'cata_type': 'default',
                '_order': '1:b'
            }
            yield FormRequest(url=self.data_list_url, formdata=next_form_data,
                          callback=self.metadata_parse, meta={'start_num': data_num})

    def file_parse(self, response):
        host = 'http://www.cddata.gov.cn/odweb/catalog/catalogDetail.htm?cata_id='
        file_data = dict()
        response_data = json.loads(response.text)
        for data in response_data['data']:
            file_data['fileId'] = data['fileId']
            file_data['fileName'] = data['fileName'] + '.' + data['fileFormat']
            file_data['metadata'] = response.meta['metadata']
            yield Request(url=host+response.meta['cata_id'], callback=self.datafield_parse, meta={'item': file_data})

    def datafield_parse(self, response):
        item = ChengduopendataItem()
        datafield = list()
        item['metadata'] = response.meta['item']['metadata']
        item['fileName'] = response.meta['item']['fileName']
        item['fileId'] = response.meta['item']['fileId']
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

