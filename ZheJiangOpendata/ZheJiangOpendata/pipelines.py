# -*- coding: utf-8 -*-
from .mysql_db.operate import session
import logging
import requests
from . import settings
import datetime
import json


class ZheJiangOpendataPipeline(object):
    def __init__(self):
        # 数据库session
        self.session = session
        # 数据总条数
        self.count = 0
        # 图像数量
        self.image_number = 0
        # 视频数量
        self.video_number = 0
        # 音频数据
        self.audio_number = 0
        # 文件数量
        self.file_number = 0
        # 图像大小（单位为kb）
        self.image_size = 0
        # 视频大小
        self.video_size = 0
        # 音频大小
        self.audio_size = 0
        # 文件大小
        self.file_size = 0

    def close_spider(self, spider):
        self.session.close()
        # 向平台发送请求， 保存数据采集量记录
        data = {
            'address': '172.16.119.3',
            'db_name': '',
            'table_name': '',
            'project_name': settings.BOT_NAME,
            'number': self.count,
            'image_number': self.image_number,
            'video_number': self.video_number,
            'audio_number': self.audio_number,
            'file_number': self.file_number,
            'image_size': self.image_size,
            'video_size': self.video_size,
            'audio_size': self.audio_size,
            'file_size': self.file_size,
        }
        res = requests.post(
            url=settings.DATARECORDADDRESS,
            data=data)
        if not res.status_code == 200:
            logging.info('关闭爬虫时错误，保存数据记录出错！')

        # 记录爬虫采集记录, 增量采集核心参数
        with open('ZheJiangOpendata/util/crawllog.json', 'w') as f:
            json.dump({"last_date": str(datetime.date.today())}, f)

    def update_data_size(self, file_type, size):
        if file_type == 'image':
            self.image_number += 1
            self.image_size += size
        elif file_type == 'video':
            self.video_number += 1
            self.video_size += size
        elif file_type == 'audio':
            self.audio_number += 1
            self.audio_size += size
        elif file_type == 'file':
            self.file_number += 1
            self.file_size += size

    def process_item(self, item, spider):
        try:
            requests.post(
                url=settings.UNSTRUCTUREDDATASTORAGEADDRESS,
                data={
                    'project_name': settings.BOT_NAME,
                    'file_name': item.get('file_name'),
                    'content': item.get('file_content')
                }
            )
            # 数据统计信息更新
            self.update_data_size(item.get('file_type'), item.get('file_size'))
            # 数据总量变量自增
            self.count += 1
        except Exception as e:
            logging.info(e, '数据存储错误, 字段类型定义出现错误，错误可能值的长度超过了字段定义的长度！')
            self.session.rollback()
