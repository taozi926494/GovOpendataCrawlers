# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import logging
import os

import requests
from scrapy.pipelines.files import FilesPipeline
from scrapy import FormRequest
from scrapy.conf import settings
from GuiYangOpendata.utils import toolkit
from GuiYangOpendata.utils.FileSummary import FilesSummary
from GuiYangOpendata.utils.LastCrawl import LastCrawl


class DatasetDownloadPipeline(FilesPipeline):
    def get_media_requests(self, item, info):
        """重写请求生成函数"""
        for file_url in item['file_urls']:
            yield FormRequest(file_url, formdata=item['formdata'], meta={
                "dataset_name": item['metadata']['名称']
            })

    def file_path(self, request, response=None, info=None):
        """重写保存路径函数"""
        return '%s/file.zip' % request.meta['dataset_name']

    def item_completed(self, results, item, info):
        """重写下载完成钩子函数"""
        dir_name = 'files/%s' % item['metadata']['名称']
        zip_file = '%s/file.zip' % dir_name
        metadata_json_file = '%s/metadata.json' % dir_name
        # 解压文件并删除安装包
        toolkit.un_zip(file_name=zip_file, dir_name=dir_name)
        os.remove(zip_file)
        # 写入元数据json
        with open(metadata_json_file, 'w', encoding='utf8') as f:
            json.dump(item['metadata'], f, ensure_ascii=False)

        return item

    def close_spider(self, spider):
        dataset_count = FilesSummary.dataset_count()
        size_mb = FilesSummary.size_mb()

        data = {
            'address': '172.16.119.3',
            'project_name': settings.get('BOT_NAME'),
            'file_number': dataset_count - LastCrawl.dataset_count(),
            'file_size': size_mb - LastCrawl.total_file_size_mb(),
        }
        print(data)
        # res = requests.post(
        #     url=settings.get('DATARECORDADDRESS'),
        #     data=data)
        # if not res.status_code == 200:
        #     logging.info('关闭爬虫时错误，保存数据记录出错！')

        LastCrawl.write(dataset_count=dataset_count, total_file_size_mb=size_mb)



