from scrapy.utils.response import get_base_url
from urllib.parse import urljoin
from scrapy.http import Response
import logging
import time


def unify_url(response, url):
    """
    标准化url路径 （绝对路径和相对路径都会转换成绝对路径）
    :param response: scrapy.Response
    :param url: url
    :return:
    """
    if not isinstance(response, Response):
        logging.error("[** ERROR **] function unify_path(response, url) "
                      "papram response is not instance of scrapy.Response")
    return urljoin(get_base_url(response), url)


def date2timestamp(date_str):
    """
    将日期格式转换为时间戳
    :param date_str: 日期字符串类型
    :return: 返回int 类型的时间戳
    """
    # 定义格式
    date = time.strptime(date_str, "%Y-%m-%d")
    return int(time.mktime(date))
