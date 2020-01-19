# -*- coding: utf-8 -*-
import json
import re
import logging
import scrapy
from scrapy import Request, FormRequest
from ..items import FileDownloadItem
from ..utils.LastCrawl import LastCrawl
from ..utils.FileSummary import FilesSummary
from lxml import etree
from ..utils import toolkit
import time


class MasterSpider(scrapy.Spider):
    name = 'master'
    last_crawl_time = 0
    headers = {
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
        "ZJZWFWSESSIONID": "54810346-b412-40a3-a2af-5bcb18d8298e",
        "zjzwfwlogin": "demo_",
        "SERVERID": "6943c432294a088664f7a77dc414f853|{}|1579401737".format(int(time.time()))
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 上次的采集时间，如果为1483200000（2017-01-1），则设置为指定初始值， 表示首次采集
        self.last_crawl_date = LastCrawl.crawl_time()
        # 首次爬去的标志位
        self.first_crawl = True if self.last_crawl_date == 1483200000 else False

    def start_requests(self) -> 'Request':
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
                headers=self.headers,
                cookies=self.cookies,
                callback=self.parse_list
            )

    def parse_list(self, response)-> 'FileDownloadItem':
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
            update_timestamp = toolkit.date_to_stamp(update)
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
                    attachment_url = toolkit.unify_url(response, url)
                    # 更新时间cookies的时间参数， token的有效时间为180s
                    self.cookies['SERVERID'] = "6943c432294a088664f7a77dc414f853|{}|1579246656".format(int(time.time()))
                    # 请求详情页
                    yield scrapy.Request(
                        url=attachment_url,
                        headers=self.headers,
                        cookies=self.cookies,
                        callback=self.parse_dataset
                    )
                else:
                    logging.info('在列表页进行数据下载，非首次采集！')
                    # 获取文件名
                    file_name = item.xpath('div/div/p[@class="search_result_left_tit"]/a/text()')
                    file_name = file_name[0] if file_name else None
                    if not file_name:
                        continue
                    # 提取url
                    attachment_urls = item.xpath('div/div/p[@class="search_result_left_stit1"]/a/@href')
                    for _url in attachment_urls:
                        url = toolkit.unify_url(response, _url)
                        # 更新时间cookies的时间参数， token的有效时间为180s
                        self.cookies['SERVERID'] = "6943c432294a088664f7a77dc414f853|{}|1579246656".format(
                            int(time.time()))
                        file_download_item = FileDownloadItem()
                        file_download_item['file_urls'] = [url]
                        file_download_item['metadata'] = {
                            "名称": file_name,
                            "headers": self.headers,
                            "cookies": self.cookies,
                        }
                        # yield 文件下载的item，到FilePipeline中去下载文件
                        yield file_download_item
            else:
                logging.info("后续数据的更新时间小于上次采集时间，数据已经采集过, 停止采集！")
                return None

    def parse_dataset(self, response) -> 'FileDownloadItem':
        """详情页的解析"""
        # 文件名
        file_name = response.xpath('//span[@class="sjxqTit1"]/text()').extract_first()
        # 提取当前文件以及历史文件的所有a标签的href
        current_download = response.xpath('//td[@class="sjxz1"]/following-sibling::td[1]/a/@href').extract()
        # 遍历提取的所有文件url
        for _url in current_download:
            # 将文件的url的相对路径转换为绝对路径
            url = toolkit.unify_url(response, _url)
            # 由于一个数据有多种格式，需要将所有数据保存在文件夹下，故以文件名作为文件夹，路径拼接
            relative_path = '/'.join([file_name, file_name])
            # 文件格式
            file_type = re.findall(r'fileType=(.+?)&', url)[0]
            # 判断url是否为历史数据，
            path = relative_path + str(int(time.time() * 100000)) + '.' + file_type
            # 更新时间cookies的时间参数， token的有效时间为180s
            self.cookies['SERVERID'] = "6943c432294a088664f7a77dc414f853|{}|1579246656".format(int(time.time()))
            if '&history=' in url:
                _file_name = file_name+"/history"
            else:
                _file_name = file_name
            file_download_item = FileDownloadItem()
            file_download_item['file_urls'] = [url]
            file_download_item['metadata'] = {
                "名称": _file_name,
                "headers": self.headers,
                "cookies": self.cookies,
            }
            # yield 文件下载的item，到FilePipeline中去下载文件
            yield file_download_item




