import logging

import requests
import random
from datetime import datetime
from bs4 import BeautifulSoup
import threading
from six.moves import urllib
import socket

logger = logging.getLogger("lianjia")

logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s(): %(lineno)d - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

hds = [{'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},
       {
           'User-Agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},
       {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},
       {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},
       {
           'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},
       {
           'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},
       {
           'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},
       {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},
       {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},
       {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},
       {
           'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},
       {'User-Agent': 'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},
       {'User-Agent': 'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}]

hd = {
    'Host': 'bj.lianjia.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Referer': 'http://captcha.lianjia.com/?redirect=http%3A%2F%2Fbj.lianjia.com%2Fxiaoqu%2Fxicheng%2F',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'zh-CN,zh;q=0.8',
    'Cookie': 'lianjia_uuid=31746070-1dfd-441b-9dac-a762d90294a5; UM_distinctid=15b12d80b45179-0d64c7a3f6681e-1d396853-1fa400-15b12d80b46247; introduce=1; all-lj=c28812af28ef34a41ba2474a2b5c52c2; select_city=110000; _jzqx=1.1490669800.1491529315.3.jzqsr=captcha%2Elianjia%2Ecom|jzqct=/.jzqsr=captcha%2Elianjia%2Ecom|jzqct=/; _jzqckmp=1; CNZZDATA1253477573=1526314437-1490666871-http%253A%252F%252Fcaptcha.lianjia.com%252F%7C1491525581; _smt_uid=58d9d0e8.bf2821b; CNZZDATA1254525948=497550824-1490668493-http%253A%252F%252Fcaptcha.lianjia.com%252F%7C1491527170; CNZZDATA1255633284=1227338008-1490665030-http%253A%252F%252Fcaptcha.lianjia.com%252F%7C1491529075; CNZZDATA1255604082=1285546817-1490665213-http%253A%252F%252Fcaptcha.lianjia.com%252F%7C1491529283; _qzja=1.866324558.1490669800393.1490941575494.1491529315136.1491529677322.1491530677583.0.0.0.54.10; _qzjb=1.1491529315136.4.0.0.0; _qzjc=1; _qzjto=4.1.0; _jzqa=1.1305601964964521000.1490669800.1490941575.1491529315.10; _jzqc=1; _jzqb=1.4.10.1491529315.1; _gat=1; _gat_past=1; _gat_global=1; _gat_new_global=1; _ga=GA1.2.48956529.1490669802; _gat_dianpu_agent=1; lianjia_ssid=6fa2fc72-0887-4093-aab6-2345792b86d3'
}


def get_html_content(url):
    try:
        result = requests.get(
            url, headers=hds[random.randint(0, len(hds) - 1)])
        html_content = result.content
    except Exception as e:
        print(e)
        return

    return html_content
