#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: Evan
# Date: 2020/1/19

# 工具包
import re
import os
import time
import zipfile



def un_zip(file_path: str, dest_dir: str):
    """
    解压zip文件
    :param file_path: 文件路径
    :param dest_dir: 解压后的目标文件夹
    :return:
    """
    zip_file = zipfile.ZipFile(file_path)
    for filename in zip_file.namelist():
        try:
            # windows
            new_filename = filename.encode('cp437').decode('gbk')
        except:
            # linux
            new_filename = filename.encode('utf-8').decode('utf-8')
        zip_file.extract(filename, dest_dir)
        # os.rename(dest_dir + filename, dest_dir + new_filename)
    zip_file.close()

def remove_blanks(s: str) -> str:
    """去掉字符串中的空格"""
    return re.sub('\s+|\n+', '', s)


def date_to_stamp(d: str) -> int:
    """日期转时间戳"""
    time_arr = time.strptime(d, "%Y-%m-%d")
    return int(time.mktime(time_arr))
