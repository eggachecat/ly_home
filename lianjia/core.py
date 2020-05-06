import os
import time
import traceback
import urllib
from datetime import datetime
from shutil import copyfile

from bs4 import BeautifulSoup
from tqdm import tqdm

from db.model import database, HouseInfoModel, HistoricalPriceModel, database_init, RentInfoModel
from lianjia.utils import get_html_content, logger

ER_SHOU_FANG_PRICE_FILTERS = [f"p{i}" for i in range(1, 8)]
ER_SHOU_FANG_ROOM_FILTERS = [f"l{i}" for i in range(1, 7)]

ER_SHOU_FANG_FILTERS = [
    ER_SHOU_FANG_PRICE_FILTERS,
    ER_SHOU_FANG_ROOM_FILTERS
]

ZU_FANG_RENT_PRICE_FILTERS = [f"rp{i}" for i in range(1, 8)]
ZU_FANG_FILTERS = [
    ZU_FANG_RENT_PRICE_FILTERS
]


def make_url(base_url, region, parameters):
    return base_url + "{region}/{parameters}/".format(
        region=region, parameters="".join(parameters)
    ).replace("//", "/")


def strip_list(content_list):
    for i, raw_content in enumerate(content_list):
        while "  " in raw_content:
            raw_content = raw_content.replace("  ", " ")
        content_list[i] = raw_content.strip()


def load_cache(prefix, region):
    date = datetime.now().strftime("%Y-%m-%d")
    cache_file = f"../.cache/{date}_{prefix}_url_candidates_{region}"
    if os.path.exists(cache_file):
        with open(cache_file, "r") as fp:
            candidate_urls = [line.strip() for line in fp.readlines()]
        return candidate_urls
    return None


def save_cache(prefix, region, cache):
    date = datetime.now().strftime("%Y-%m-%d")
    cache_file = f"../.cache/{date}_{prefix}_url_candidates_{region}"
    if os.path.exists(cache_file) and not os.path.exists(f"{cache_file}_all"):
        copyfile(cache_file, f"{cache_file}_all")

    with open(cache_file, "w") as fp:
        fp.write("\n".join(cache))


class BaseCrawler:
    """
            抽象了一些公共方法
    """

    def __init__(self, base_url, filters):
        self.base_url = base_url
        self.filters = filters

    def get_number_of_pages(self, *args, **kwargs):
        raise NotImplemented()

    def get_candidate_urls(self, urls, region, filters=None, filter_level=0):
        if filters is None:
            filters = []
        url = make_url(self.base_url, region, filters)
        logger.debug(f"Visiting {url}")
        number_of_pages = self.get_number_of_pages(
            BeautifulSoup(get_html_content(url), 'lxml'))
        logger.debug(f"#pages {number_of_pages}")
        if number_of_pages >= 100 and filter_level < len(self.filters):
            for f in self.filters[filter_level]:
                self.get_candidate_urls(urls, region, filters + [f], filter_level + 1)
        else:
            if number_of_pages >= 100 and filter_level >= len(self.filters):
                logger.debug(f"{url} can NOT find all!!")
            else:
                logger.debug(f"{url} can find all!!")

            for i in range(min([
                number_of_pages, 100  # 过滤器不够的到100到情况
            ])):
                urls.append(make_url(self.base_url, region, [f"pg{i + 1}"] + filters))


class LianjiaErShouFangCrawler(BaseCrawler):
    """
        暂时就是列表的信息
        后续我们还可以整一个detail的
    """

    def __init__(self, city):
        super().__init__(
            f"http://{city}.lianjia.com/ershoufang/", ER_SHOU_FANG_FILTERS)
        self.city = city

    def get_number_of_pages(self, soup):
        try:
            page_info = soup.find('div', {'class': 'page-box house-lst-page-box'})
            page_info_str = page_info.get('page-data').split(',')[0]
            total_pages = int(page_info_str.split(':')[1])
            return total_pages
        except:
            return 0

    def parse_html(self, html, default_info=None):
        house_info_data_source = []
        historical_price_data_source = []
        soup = BeautifulSoup(html, 'lxml')
        for ul_tag in soup.findAll("ul", {"class": "sellListContent"}):
            for item in ul_tag.find_all('li'):
                info_dict = {}
                if default_info is not None:
                    info_dict.update(default_info)
                try:
                    house_title = item.find("div", {"class": "title"})
                    info_dict.update(
                        {'title': house_title.a.get_text().strip()})
                    info_dict.update({'link': house_title.a.get('href')})
                    house_id = house_title.a.get('data-housecode')
                    info_dict.update({'house_id': house_id})

                    house_info = item.find("div", {"class": "houseInfo"})
                    info = house_info.get_text().split('|')
                    # info_dict.update({'community': info[0]})
                    info_dict.update({'house_type': info[0]})
                    info_dict.update({'square': info[1]})
                    info_dict.update({'direction': info[2]})
                    info_dict.update({'decoration': info[3]})
                    info_dict.update({'floor': info[4]})
                    info_dict.update({'years': info[5]})

                    house_floor = item.find("div", {"class": "positionInfo"})
                    community_info = house_floor.get_text().split('-')
                    info_dict.update({'community': community_info[0]})
                    if len(community_info) >= 2:
                        info_dict.update({'zone': community_info[1]})

                    follow_info = item.find("div", {"class": "followInfo"})
                    info_dict.update(
                        {'follow_info': follow_info.get_text().strip()})

                    tax_free = item.find("span", {"class": "taxfree"})
                    if tax_free is None:
                        info_dict.update({"tax_type": ""})
                    else:
                        info_dict.update(
                            {"tax_type": tax_free.get_text().strip()})

                    total_price = item.find("div", {"class": "totalPrice"})
                    info_dict.update(
                        {'total_price': total_price.span.get_text()})

                    unit_price = item.find("div", {"class": "unitPrice"})
                    info_dict.update(
                        {'unit_price': unit_price.get("data-price")})
                except:
                    continue

                house_info_data_source.append(info_dict)
                historical_price_data_source.append(
                    {"house_id": info_dict["house_id"], "total_price": info_dict["total_price"]})

        return house_info_data_source, historical_price_data_source

    def get_home_info_for_region(self, region):
        """
            对于每个区的二手房的爬虫
        """
        candidate_urls = load_cache("ershoufang", region)
        if candidate_urls is None:
            candidate_urls = []
            self.get_candidate_urls(candidate_urls, region)
            save_cache("ershoufang", region, candidate_urls)

        for i, url in enumerate(tqdm(candidate_urls)):
            house_info_data_source, historical_price_data_source = self.parse_html(
                html=get_html_content(url), default_info={"region": region})
            with database.atomic():
                if house_info_data_source:
                    HouseInfoModel.insert_many(house_info_data_source).on_conflict_replace().execute()
                if historical_price_data_source:
                    HistoricalPriceModel.insert_many(
                        historical_price_data_source).on_conflict_replace().execute()
            save_cache("ershoufang", region, candidate_urls[i + 1:])
            time.sleep(1)


class LianjiaZuFangCrawler(BaseCrawler):
    """
        租房的爬虫
        现在也是从列表来
    """

    def __init__(self, city):
        super().__init__(
            f"http://{city}.lianjia.com/zufang/", ZU_FANG_FILTERS)
        self.city = city

    def get_number_of_pages(self, soup):
        try:
            page_navigation = soup.find('div', {'data-el': 'page_navigation'})
            return int(page_navigation.get('data-totalpage'))
        except:
            return 0

    def parse_html(self, html, default_info=None):
        rent_info_data_source = []
        soup = BeautifulSoup(html, 'lxml')

        for list_item in soup.findAll("div", {"class": "content__list--item", "data-house_code": True}):
            try:
                info_dict = {
                    "rent_type": "",
                    "decoration": "",
                    "subway": ""
                }

                if default_info is not None:
                    info_dict.update(default_info)

                house_id = list_item.get('data-house_code')
                info_dict.update({'house_id': house_id})

                house_ref = list_item.find("p", {"class": "content__list--item--title twoline"})
                house_title = house_ref.a.get_text().strip()
                info_dict.update({'title': house_title})

                if "·" in house_title:
                    info_dict.update({"rent_type": house_title.split("·")[0]})
                    # if "室" in house_title and "厅" in house_title:
                    #     # todo: 正则表达式
                    #     pass

                info_dict.update({'link': urllib.parse.urljoin(self.base_url, house_ref.a.get('href'))})

                description = list_item.find("p", {"class", "content__list--item--des"})
                description_list = description.get_text().strip().split("\n")
                strip_list(description_list)

                location_info = description_list[0].split("-")
                info_dict.update({"region": location_info[0]})
                info_dict.update({"zone": location_info[1]})
                info_dict.update({"community": location_info[2]})

                info_dict.update({"square": description_list[2]})
                info_dict.update({"direction": description_list[3].replace("/", "").strip()})
                info_dict.update({"house_type": description_list[4]})
                info_dict.update({"floor": description_list[6]})

                info_dict.update(
                    {"price": list_item.find("span", {"class": "content__list--item-price"}).get_text().strip()})

                decoration_field = list_item.find("i", {"class": "content__item__tag--decoration"})
                if decoration_field is not None:
                    info_dict.update({"decoration": decoration_field.get_text().strip()})

                subway_field = list_item.find("i", {"class": "content__item__tag--is_subway_house"})
                if subway_field is not None:
                    info_dict.update({"subway": subway_field.get_text().strip()})
                rent_info_data_source.append(info_dict)
            except:
                traceback.print_exc()
                continue
        return rent_info_data_source

    def get_rent_info_for_region(self, region):
        candidate_urls = load_cache("zufang", region)
        if candidate_urls is None:
            candidate_urls = []
            self.get_candidate_urls(candidate_urls, region)
            save_cache("zufang", region, candidate_urls)

        for i, url in enumerate(tqdm(candidate_urls)):
            rent_info_data_source = self.parse_html(
                html=get_html_content(url), default_info={"region": region})
            with database.atomic():
                if rent_info_data_source:
                    RentInfoModel.insert_many(rent_info_data_source).on_conflict_replace().execute()
            save_cache("zufang", region, candidate_urls[i + 1:])
            time.sleep(1)


if __name__ == '__main__':
    database_init()
    region_list = [
        "pudong",  # 浦东
        "jingan",  # 静安
        "xuhui",  # 徐汇
        "huangpu",  # 黄浦
        "changning",  # 长宁
        "putuo",  # 普陀
        "baoshan",  # 宝山
        "hongkou",  # 虹口
        "yangpu",  # 杨浦
        "minhang",  # 闵行
        "jinshan",  # 金山
        "jiading",  # 嘉定
        "chongming",  # 崇明
        "fengxian",  # 奉贤
        "songjiang",  # 松江
        "qingpu"  # 青浦
    ]
    for region in region_list:
        ershoufang_crawler = LianjiaErShouFangCrawler("sh")
        ershoufang_crawler.get_home_info_for_region(region)
        zufang_crawler = LianjiaZuFangCrawler("sh")
        zufang_crawler.get_rent_info_for_region(region)
