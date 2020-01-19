# -*- coding: utf-8 -*-
import scrapy
from ..items import FileItem
from ..util.LastCrawl import LastCrawl
from ..util import toolkit
from ..util import parse_err
import json


class KfgdSpider(scrapy.Spider):
    # 爬虫名
    name = 'kfgd'
    updateCycle = {
        '1': '实时更新', '2': '按日更新', '3': '按周更新', '4': '按月更新',
        '5': '每季度', '6': '每半年', '7': '按年更新', '99': '其他'
    }
    page = 1
    last_crawl_time = 0

    def start_requests(self):
        """
        爬虫起点函数，重写父类方法
        :return: None
        """
        self.last_crawl_time = LastCrawl.crawl_time()
        url = 'http://gddata.gd.gov.cn/data/dataSet/findPage'
        form_data = {
            'pageNo': '1',
            'pageSize': '10',
            'sortType': 'DESC',
            'classifyId': ''
        }
        yield scrapy.FormRequest(
            url=url,
            formdata=form_data,
            callback=self.only_unstructured_parse,
            dont_filter=False
        )

    def only_unstructured_parse(self, response):
        """
        解析详情页, 采集数据仅有非结构化类型数据
        :param response: 请求详情页后返回的 response
        :return: None
        """
        dataset_crawled = 0
        table_list = json.loads(json.loads(response.body.decode())['dataList'])
        data_list = table_list['list']

        if len(data_list) == 0:
            # 打印详情页列表未找到错误
            parse_err.log(parse_err.NO_LIST, 'paper_list', '数据集列表', response)
            return
        for data in data_list:
            update_time = toolkit.date_to_stamp(data.get('dataUpdateTime'))
            # 如果更新时间大于上次爬取的时间 则下载数据集
            if update_time > self.last_crawl_time:
                yield scrapy.FormRequest(
                    url='http://gddata.gd.gov.cn/data/catalog/selectDataCatalogByResId',
                    formdata={'resId': data['resId']},
                    callback=self.detail_parse,
                    dont_filter=False
                )
            else:
                dataset_crawled += 1

        # 每页10条数据，如果有超过10条数据被爬过，就不再翻页
        if dataset_crawled >= 10:
            return
        else:
            self.page += 1
            yield scrapy.FormRequest(
                url='http://gddata.gd.gov.cn/data/dataSet/findPage',
                formdata={'pageNo': str(self.page), 'pageSize': '10', 'sortType': 'DESC', 'classifyId': ''},
                callback=self.only_unstructured_parse,
                dont_filter=False
            )

    def detail_parse(self, response):
        data = json.loads(response.body.decode())
        metadata = {}
        title = data['resTitle']
        resId = data['resId']
        metadata['标识符'] = resId
        metadata['名称'] = title
        metadata['简介'] = data['resAbstract']
        metadata['关键字'] = data['keyword']
        metadata['主题分类'] = data['subjectName']
        metadata['开放方式'] = data['openMode']
        metadata['更新频率'] = self.updateCycle.get(data['updateCycle'])
        metadata['发布日期'] = data['publishDate']
        metadata['更新日期'] = data['dataUpdateTime']
        metadata['资源格式'] = data['sourceSuffix']
        metadata['所属行政区域'] = data['officeName']
        metadata['数据提供方'] = data['officeName']
        metadata['提供方地址'] = data['address']
        metadata['数据维护方'] = data['officeName']
        metadata['文件大小'] = data['fileSize']
        metadata['数据量'] = data['recordTotal']
        metadata['文件数量'] = data['fileCount']

        yield scrapy.FormRequest(
            url='http://gddata.gd.gov.cn/data/dataSet/getFileList',
            formdata={'resId': resId, 'sourceSuffix': '', 'beginDate': '', 'endDate': ''},
            callback=self.file_parse,
            meta={'resId': resId.replace('/', '_'), 'title': title, 'metadata': str(metadata)},
            dont_filter=False
        )

    def file_parse(self, response):
        item = FileItem()
        file_list = json.loads(response.body.decode())
        file_urls = []
        file_name = []
        # fileName中已经包含文件后缀
        for file in file_list:
            url = 'http://gddata.gd.gov.cn/downloadFiles/open-file/%s/%s'\
                  % (response.meta['resId'], file['fileUrl'])
            file_urls.append(url)
            file_name.append(file['fileName'])
        item['file_urls'] = file_urls
        item['title'] = response.meta['title']
        item['file_names'] = file_name
        item['metadata'] = response.meta['metadata']
        yield item
