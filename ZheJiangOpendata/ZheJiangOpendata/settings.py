# -*- coding: utf-8 -*-
import socket
# 工程名
BOT_NAME = 'ZheJiangOpendata'
SPIDER_MODULES = ['ZheJiangOpendata.spiders']
NEWSPIDER_MODULE = 'ZheJiangOpendata.spiders'

# 爬虫机器协议，即那些内容可爬取， 那些不可以
ROBOTSTXT_OBEY = False
# 设置日志
# LOG_LEVEL = 'INFO'
# 爬虫的并行请求量， 默认16
CONCURRENT_REQUESTS = 8
# 爬虫的下载延迟
DOWNLOAD_DELAY = 0.5
# spider中间件
SPIDER_MIDDLEWARES = {
   'ZheJiangOpendata.middlewares.ZheJiangOpendataSpiderMiddleware': 543,
}
# 下载中间件
DOWNLOADER_MIDDLEWARES = {
  'ZheJiangOpendata.middlewares.ZheJiangOpendataDownloaderMiddleware': 543,
}
# item 与 pipelines启动设置
ITEM_PIPELINES = {
   'ZheJiangOpendata.pipelines.ZheJiangOpendataPipeline': 300,
}
# 自定义数据采集调度平台数据采集记录系统接口地址
DATARECORDADDRESS = 'http://172.16.119.6:5000/add_record'

# 非结构化数据（文件、视频、音频、图像 4类）存储接口地址
UNSTRUCTUREDDATASTORAGEADDRESS = 'http://172.16.119.3:1213/savefile'

# 数据采集类型（STRUCTURED：仅结构化， UNSTRUCTURED: 仅（视频、图像、音频、文件）等非结构化；BOTH二者都有）
COLLECTDATATYPE = 'STRUCTURED'

# 代码所处阶段，分为开发环境（development）与生产环境(production)两个阶段，开发环境表示调试阶段， 生产环境表示正常运行阶段
hostname = socket.gethostname()  # 获取计算机名称
# 获取本机IP
ip = socket.gethostbyname(hostname)
# 如果代码是在服务器上运行， 则连接的是服务器的地址， 否则连接的是自己本地数据库的地址
ENVIRINMENT = 'production' if '172.16.119' in ip else 'development'
