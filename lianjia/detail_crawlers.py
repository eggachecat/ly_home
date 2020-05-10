import threading
import time

import tqdm
from bs4 import BeautifulSoup

from db.model import HouseInfoModel, database_init, database, SubwayCommunityModel, CommunityModel
from lianjia.info_crawlers import load_cache, save_cache
from lianjia.utils import get_html_content, run_with_threads, check_block

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

"""
     由于需要用selenium下载
     我们先多线程现在然后再处理
"""


#
# class LianjiaErShouFangDetailCrawler:
#     def __init__(self):
#         self.lock_flag = 0
#
#     def parse_html(self, html, default_info=None, default_subway_info=None):
#         if default_subway_info is None:
#             default_subway_info = {}
#
#         house_detail_data_source = []
#         subway_data_source = []
#         soup = BeautifulSoup(html, 'lxml')
#
#         # 周边
#         around_section = soup.find("div", {"id": "around"})
#         subway_sections = around_section.findAll("li", {"data-index": True})
#         for subway_section in subway_sections:
#             if "subway" in subway_section.get("data-index"):
#                 try:
#                     subway_name = subway_section.find("div", {"class": "itemInfo"}).get_text().strip()
#                     subway_stop_name = subway_section.find("span", {"class": "itemTitle"}).get_text().strip()
#                     distance = subway_section.find("span", {"class": "itemdistance"}).get_text().strip()
#                     subway_data_source.append({
#                         **default_subway_info,
#                         "subway_name": subway_name,
#                         "subway_stop_name": subway_stop_name,
#                         "subway_distance": distance
#                     })
#                 except:
#                     continue
#         # print(house_detail_data_source)
#         # print(subway_data_source)
#         return house_detail_data_source, subway_data_source
#
#     def get_home_detail(self):
#         candidate_urls = load_cache("house", "detail")
#         if candidate_urls is None:
#             candidate_urls = [r['link'] for r in CommunityModel.select().dicts()]
#             save_cache("house", "detail", candidate_urls)
#
#         n_total = len(candidate_urls)
#         progress_bar = tqdm.tqdm(total=n_total + 1)
#         lock = threading.Lock()
#         self.lock_flag = 0
#
#         def _t():
#             chrome_options = Options()
#             chrome_options.add_argument("--headless")
#             browser = webdriver.Chrome(options=chrome_options)
#
#             while len(candidate_urls) > 0:
#                 url = candidate_urls.pop(0)
#                 progress_bar.update(1)
#                 house_id = url.split("/")[-1].split(".")[0]
#                 browser.get(url)
#                 html = browser.execute_script("return document.getElementsByTagName('html')[0].innerHTML")
#                 _, subway_data_source = self.parse_html(html=html, default_subway_info={"house_id": house_id})
#                 with database.atomic():
#                     if subway_data_source:
#                         SubwayHomeModel.insert_many(subway_data_source).on_conflict_replace().execute()
#                 lock.acquire()
#                 self.lock_flag += 1
#                 if self.lock_flag % 10 == 0:
#                     # 每10个保存一次
#                     save_cache("house", "detail", candidate_urls)
#                 lock.release()
#                 time.sleep(1)
#
#         run_with_threads(_t, 1)
#

class LianjiaXiaoquDetailCrawler:
    """
        小区的详情爬虫
    """

    def __init__(self):
        self.lock_flag = 0

    def parse_html(self, html, default_info=None, default_subway_info=None):
        if default_subway_info is None:
            default_subway_info = {}

        subway_data_source = []
        soup = BeautifulSoup(html, 'lxml')

        # 基本信息
        if check_block(soup):
            return
        _community_infos = soup.findAll("div", {"class": "xiaoquInfoItem"})
        community_detail_info = {}
        for _info in _community_infos:
            key_type = {
                "建筑年代": 'year',
                "建筑类型": 'house_type',
                "物业费用": 'cost',
                "物业公司": 'service',
                "开发商": 'company',
                "楼栋总数": 'building_num',
                "房屋总数": 'house_num',
            }
            try:
                _key = _info.find("span", {"xiaoquInfoLabel"})
                _value = _info.find("span", {"xiaoquInfoContent"})
                _key_info = key_type[_key.get_text().strip()]
                _value_info = _value.get_text().strip()
                community_detail_info.update({_key_info: _value_info})
            except:
                continue

        # 周边
        around_section = soup.find("div", {"id": "around"})
        subway_sections = around_section.findAll("li", {"data-index": True})
        for subway_section in subway_sections:
            if "subway" in subway_section.get("data-index"):
                try:
                    subway_name = subway_section.find("div", {"class": "itemInfo"}).get_text().strip()
                    subway_stop_name = subway_section.find("span", {"class": "itemTitle"}).get_text().strip()
                    distance = subway_section.find("span", {"class": "itemdistance"}).get_text().strip()
                    subway_data_source.append({
                        **default_subway_info,
                        "subway_name": subway_name,
                        "subway_stop_name": subway_stop_name,
                        "subway_distance": distance
                    })
                except:
                    continue
        return community_detail_info, subway_data_source

    def get_community_detail(self):
        candidate_urls = load_cache("xiaoqu", "detail")
        if candidate_urls is None:
            candidate_urls = [r['link'] for r in CommunityModel.select().dicts()]
            save_cache("xiaoqu", "detail", candidate_urls)

        n_total = len(candidate_urls)
        progress_bar = tqdm.tqdm(total=n_total + 1)
        lock = threading.Lock()
        self.lock_flag = 0

        def _t():
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            browser = webdriver.Chrome(options=chrome_options)

            while len(candidate_urls) > 0:
                url = candidate_urls.pop(0)
                if url[-1] == "/":
                    url = url[:-1]
                progress_bar.update(1)
                community_id = url.split("/")[-1].split(".")[0]
                browser.get(url)
                time.sleep(5)
                html = browser.execute_script("return document.getElementsByTagName('html')[0].innerHTML")
                community_info, subway_data_source = self.parse_html(html=html,
                                                                     default_subway_info={"community_id": community_id})
                with database.atomic():
                    if subway_data_source:
                        SubwayCommunityModel.insert_many(subway_data_source).on_conflict_replace().execute()
                    if community_info:
                        CommunityModel.update({**community_info}).where(
                            CommunityModel.id == community_id).execute()
                lock.acquire()
                self.lock_flag += 1
                if self.lock_flag % 10 == 0:
                    # 每10个保存一次
                    save_cache("xiaoqu", "detail", candidate_urls)
                lock.release()
                time.sleep(1)

        run_with_threads(_t, 1)


if __name__ == '__main__':
    database_init()
    xiaoqu_detail_crawler = LianjiaXiaoquDetailCrawler()
    xiaoqu_detail_crawler.get_community_detail()
    # ershoufang_detail_crawler = LianjiaErShouFangDetailCrawler()
    # ershoufang_detail_crawler.get_home_detail()
    exit()
