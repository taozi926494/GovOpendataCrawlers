# -*- coding: utf-8 -*-
import scrapy
from scrapy.loader import ItemLoader
from ..items import FileItem
import base64
import re
from lxml import etree
from ..util.common import unify_url, date2timestamp
import time
import logging
import json


class ZjSpider(scrapy.Spider):
    # 爬虫名
    name = 'zj'
    header = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        " Accept-Language": "zh-CN,zh;q=0.9",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Host": "data.zjzwfw.gov.cn",
        "Referer": "http://data.zjzwfw.gov.cn/jdop_front/channal/data_public.do?deptId=24&domainId=0",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36"
    }
    cookies = {
        "visited": "1",
        "ZJZWFWSESSIONID": "de67be4a-ca3f-4a9e-963f-3a35208492ed",
        "zjzwfwlogin": "demo_",
        "SERVERID": "6943c432294a088664f7a77dc414f853|{}|1579329132".format(int(time.time()))
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        with open('ZheJiangOpendata/util/crawllog.json', 'r') as f:
            temp = json.load(f)
        # 判断是否为首次爬取， 如果temp中没有时间记录，则为首次爬取
        if temp.get('last_date') == "":
            # 首次爬去的标志位
            self.first_crawl = True
            # 上次的采集时间，如果为空，则设置为指定初始值
            self.last_crawl_date = date2timestamp('2019-01-01')
        else:
            self.first_crawl = False
            self.last_crawl_date = date2timestamp(temp.get('last_date'))

    def start_requests(self):
        url = 'http://data.zjzwfw.gov.cn/jdop_front/channal/datapubliclist.do'
        formdata = {
            "pageNumber": "1",
            "pageSize": "600",
            "typ": "1",
            "domainId": "0",
            "regionId": "shengji",
            "isDownload": "1",
            "orderField": "2"
        }
        for i in range(1, 2):
            formdata['pageNumber'] = str(i)
            yield scrapy.FormRequest(
                url=url,
                formdata=formdata,
                headers=self.header,
                cookies=self.cookies,
                callback=self.only_unstructured_parse
            )

    def only_unstructured_parse(self, response):
        """
        解析详情页, 采集数据仅有非结构化类型数据
        :param response: 请求详情页后返回的 response
        :return: None
        """
        if response.status != 200:
            logging.info("首页请求不到数据请检查cookies的有效性或者请重试！")
            return
        # 获取数据json格式
        data = json.loads(response.text).get('data')
        # 对返回的html数据格式化， 便于利用xpath
        _response = etree.HTML(data)
        # 提取所有的数据item
        list_items = _response.xpath("//div[@class='search_result']")
        # 遍历数据列表
        for item in list_items:
            # 获取数据的更新时间
            update = item.xpath('div/div[2]/p[@class="ck"]/span[3]/span/text()')
            # 如果未提取到更新日期
            if not update:
                logging.info("没有提取到更新日期，已跳过该条记录的解析！")
                continue
            else:
                update = update[0]
            # 将日期格式转换为时间戳
            update_timestamp = date2timestamp(update)
            # 如果数据的更新时间大于上次的爬取时间， 则表示数据为新更数据， 需要爬取
            if update_timestamp > self.last_crawl_date:
                # 如果为首次爬取，然后进入详情页进行下载， 详情页中存在历史数据，否则直接在列表页下载最新数据
                if self.first_crawl:
                    logging.info('首次下载，进入详细页进行数据下载！')
                    # 获取详情页url，
                    url = item.xpath('div/div/p[@class="search_result_left_tit"]/a/@href')
                    # 格式化url
                    url = url[0] if url else None
                    # 如果没有提取到文件名或者url,则跳过该条记录
                    if not url:
                        continue
                    # 将提取的url转换为绝对路径
                    attachment_url = unify_url(response, url)
                    # 更新时间cookies的时间参数， token的有效时间为180s
                    self.cookies['SERVERID'] = "6943c432294a088664f7a77dc414f853|{}|1579246656".format(int(time.time()))
                    # 请求详情页
                    yield scrapy.Request(
                        url=attachment_url,
                        headers=self.header,
                        cookies=self.cookies,
                        callback=self.parse_detail
                    )
                else:
                    logging.info('在列表页进行数据下载，非首次采集！')
                    # 获取文件名
                    file_name = item.xpath('div/div/p[@class="search_result_left_tit"]/a/text()')
                    file_name = file_name[0] if file_name else None
                    if not file_name:
                        continue
                    # 由于一个数据有多种格式，需要将所有数据保存在文件夹下，故以文件名作为文件夹，路径拼接
                    relative_path = '/'.join([file_name, file_name])
                    # 文件类型
                    file_type = re.findall(r'fileType=(.+)&', url)[0]
                    # 提取url
                    attachment_urls = item.xpath('div/div/p[@class="search_result_left_stit1"]/a/@href')
                    for _url in attachment_urls:
                        path = relative_path + str(int(time.time()*100000))+'.' + file_type
                        url = unify_url(response, _url)
                        # 更新时间cookies的时间参数， token的有效时间为180s
                        self.cookies['SERVERID'] = "6943c432294a088664f7a77dc414f853|{}|1579246656".format(int(time.time()))
                        yield scrapy.Request(
                                url=url,
                                headers=self.header,
                                cookies=self.cookies,
                                callback=self.attachment_parse,
                                meta={"file_name": path}
                        )
            else:
                logging.info("后续数据的更新时间小于上次采集时间，数据已经采集过, 停止采集！")
                return None

    def parse_detail(self, response):
        """详情页的解析"""
        # 文件名
        file_name = response.xpath('//span[@class="sjxqTit1"]/text()').extract_first()
        # 提取当前文件以及历史文件的所有a标签的href
        current_download = response.xpath('//td[@class="sjxz1"]/following-sibling::td[1]/a/@href').extract()
        # 遍历提取的所有文件url
        for _url in current_download:
            # 将文件的url的相对路径转换为绝对路径
            url = unify_url(response, _url)
            # 由于一个数据有多种格式，需要将所有数据保存在文件夹下，故以文件名作为文件夹，路径拼接
            relative_path = '/'.join([file_name, file_name])
            # 文件格式
            file_type = re.findall(r'fileType=(.+?)&', url)[0]
            # 判断url是否为历史数据，
            path = relative_path + str(int(time.time()*100000))+'.' + file_type
            # 更新时间cookies的时间参数， token的有效时间为180s
            self.cookies['SERVERID'] = "6943c432294a088664f7a77dc414f853|{}|1579246656".format(int(time.time()))
            yield scrapy.Request(
                url=url,
                headers=self.header,
                cookies=self.cookies,
                callback=self.attachment_parse,
                meta={"file_name": path}
            )

    def attachment_parse(self, response):
        """
        附件解析函数
        :param response: 请求附件页面后返回的 response
        :return:
        """
        # 实例化附件item
        file_item = ItemLoader(item=FileItem())
        # 计算附件的大小
        file_size = int(len(response.body) / 1024)
        # 将文件bytes类型进行base64 编码
        file_content = base64.b64encode(response.body)
        file_name = response.meta.get('file_name')
        # 给附件item的各个字段赋值以及返回
        file_item.add_value('url', response.url)
        file_item.add_value('file_name', file_name)
        file_item.add_value('file_label', 'sdsad')
        file_item.add_value('file_type', 'file')
        file_item.add_value('file_size', file_size)
        file_item.add_value('file_content', file_content)
        yield file_item.load_item()
        logging.info('已经完成文件的：{}的下载'.format(file_name))
