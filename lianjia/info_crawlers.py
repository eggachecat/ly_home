import os
import time
import traceback
import urllib
from datetime import datetime
from shutil import copyfile

from bs4 import BeautifulSoup
from tqdm import tqdm

from db.model import database, HouseInfoModel, HistoricalPriceModel, database_init, RentInfoModel, CommunityModel, \
    SellInfoModel
from lianjia.utils import get_html_content, logger, check_block

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

XIAO_QU_UNIT_PRICE_FILTERS = [f"p{i}" for i in range(1, 8)]
XIAO_QU_FILTERS = [
    XIAO_QU_UNIT_PRICE_FILTERS
]

CHENG_JIAO_PRICE_FILTERS = [f"p{i}" for i in range(1, 8)]
CHENG_JIAO_ROOM_FILTERS = [f"l{i}" for i in range(1, 7)]
CHENG_JIAO_AREA_FILTERS = [f"a{i}" for i in range(1, 8)]
CHENG_JIAO_DIRECTION_FILTERS = [f"f{i}" for i in range(1, 6)]
CHENG_JIAO_FLOOR_FILTERS = [f"c{i}" for i in range(1, 4)]
CHENG_JIAO_FILTERS = [
    CHENG_JIAO_PRICE_FILTERS,
    CHENG_JIAO_AREA_FILTERS,
    CHENG_JIAO_ROOM_FILTERS,
    CHENG_JIAO_DIRECTION_FILTERS
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

    def __init__(self, base_url, filters, max_page=100):
        self.base_url = base_url
        self.filters = filters
        self.max_page = max_page

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
        if number_of_pages >= self.max_page and filter_level < len(self.filters):
            for f in self.filters[filter_level]:
                self.get_candidate_urls(urls, region, filters + [f], filter_level + 1)
        else:
            if number_of_pages >= self.max_page and filter_level >= len(self.filters):
                logger.debug(f"{url} can NOT find all!!")
            else:
                logger.debug(f"{url} can find all!!")

            for i in range(min([
                number_of_pages, self.max_page  # 过滤器不够的到100到情况
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
                    if house_id is None:
                        house_id = house_title.a.get('data-lj_action_housedel_id')
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


class LianjiaXiaoQuCrawler(BaseCrawler):
    """
        小区的列表
    """

    def __init__(self, city):
        super().__init__(f"http://{city}.lianjia.com/xiaoqu/", XIAO_QU_FILTERS, max_page=30)
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
        if default_info is None:
            default_info = {}

        community_data_source = []

        # def _get_community_info_by_url(url):
        #     _soup = BeautifulSoup(get_html_content(url), 'lxml')
        #     if check_block(_soup):
        #         return
        #     _community_infos = _soup.findAll("div", {"class": "xiaoquInfoItem"})
        #     _res = {}
        #     for _info in _community_infos:
        #         key_type = {
        #             "建筑年代": 'year',
        #             "建筑类型": 'house_type',
        #             "物业费用": 'cost',
        #             "物业公司": 'service',
        #             "开发商": 'company',
        #             "楼栋总数": 'building_num',
        #             "房屋总数": 'house_num',
        #         }
        #         try:
        #             _key = _info.find("span", {"xiaoquInfoLabel"})
        #             _value = _info.find("span", {"xiaoquInfoContent"})
        #             _key_info = key_type[_key.get_text().strip()]
        #             _value_info = _value.get_text().strip()
        #             _res.update({_key_info: _value_info})
        #         except:
        #             continue
        #     return _res

        soup = BeautifulSoup(html, 'lxml')
        for item in soup.findAll("li", {"class": "clear"}):
            info_dict = {
                **default_info,
                'city': 'sh',
            }
            try:
                community_title = item.find("div", {"class": "title"})
                title = community_title.get_text().strip('\n')
                link = community_title.a.get('href')
                info_dict.update({'title': title})
                info_dict.update({'link': link})

                district = item.find("a", {"class": "district"})
                info_dict.update({'district': district.get_text()})

                biz_circle = item.find("a", {"class": "bizcircle"})
                info_dict.update({'biz_circle': biz_circle.get_text()})

                tag_list = item.find("div", {"class": "tagList"})
                info_dict.update({'tag_list': tag_list.get_text().strip('\n')})

                on_sale = item.find("a", {"class": "totalSellCount"})
                info_dict.update(
                    {'on_sale': on_sale.span.get_text().strip('\n')})

                on_rent = item.find("a", {"title": title + "租房"})
                info_dict.update(
                    {'on_rent': on_rent.get_text().strip('\n').split('套')[0]})

                info_dict.update({'id': item.get('data-housecode')})

                price = item.find("div", {"class": "totalPrice"})
                info_dict.update({'price': price.span.get_text().strip('\n')})

                # community_info = _get_community_info_by_url(link)
                # for key, value in community_info.items():
                #     info_dict.update({key: value})
                community_data_source.append(info_dict)
            except:
                continue

        return community_data_source

    def get_community_info_for_region(self, region):
        candidate_urls = load_cache("xiaoqu", region)
        if candidate_urls is None:
            candidate_urls = []
            self.get_candidate_urls(candidate_urls, region)
            save_cache("xiaoqu", region, candidate_urls)

        for i, url in enumerate(tqdm(candidate_urls)):
            community_data_source = self.parse_html(
                html=get_html_content(url), default_info={"region": region})
            with database.atomic():
                if community_data_source:
                    CommunityModel.insert_many(community_data_source).on_conflict_replace().execute()
            save_cache("xiaoqu", region, candidate_urls[i + 1:])
            time.sleep(1)


class LianjiaChengJiaoCrawler(BaseCrawler):
    def __init__(self, city):
        super().__init__(f"http://{city}.lianjia.com/chengjiao/", CHENG_JIAO_FILTERS)
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
        if default_info is None:
            default_info = {}
        data_source = []
        soup = BeautifulSoup(html, 'lxml')

        for ul_tag in soup.findAll("ul", {"class": "listContent"}):
            for item in ul_tag.find_all('li'):
                info_dict = {
                    "turnover": "",
                    "list_price": "",
                    **default_info
                }

                try:
                    house_title = item.find("div", {"class": "title"})
                    info_dict.update({'title': house_title.get_text().strip()})
                    info_dict.update({'link': house_title.a.get('href')})
                    house_id = house_title.a.get(
                        'href').split("/")[-1].split(".")[0]
                    info_dict.update({'house_id': house_id.strip()})

                    house = house_title.get_text().strip().split(' ')
                    info_dict.update(
                        {'community': house[0].strip() if 0 < len(house) else ''})
                    info_dict.update(
                        {'house_type': house[1].strip() if 1 < len(house) else ''})
                    info_dict.update(
                        {'square': house[2].strip() if 2 < len(house) else ''})

                    house_info = item.find("div", {"class": "houseInfo"})
                    info = house_info.get_text().split('|')
                    info_dict.update({'direction': info[0].strip()})
                    info_dict.update(
                        {'decoration': info[1].strip() if 1 < len(info) else ''})

                    house_floor = item.find("div", {"class": "positionInfo"})
                    floor_all = house_floor.get_text().strip().split(' ')
                    info_dict.update({'floor': floor_all[0].strip()})
                    info_dict.update({'years': floor_all[-1].strip()})

                    total_price = item.find("div", {"class": "totalPrice"})
                    if total_price.span is None:
                        info_dict.update(
                            {'total_price': total_price.get_text().strip()})
                    else:
                        info_dict.update(
                            {'total_price': total_price.span.get_text().strip()})

                    unit_price = item.find("div", {"class": "unitPrice"})
                    if unit_price.span is None:
                        info_dict.update(
                            {'unit_price': unit_price.get_text().strip()})
                    else:
                        info_dict.update(
                            {'unit_price': unit_price.span.get_text().strip()})

                    deal_date = item.find("div", {"class": "dealDate"})
                    info_dict.update(
                        {'deal_date': deal_date.get_text().strip().replace('.', '-')})

                    turnover_item = item.find("span", {"class": "dealCycleTxt"})
                    if turnover_item is not None:
                        turnover_info = turnover_item.findAll("span")
                        if len(turnover_info) == 2:
                            info_dict.update({
                                "list_price": turnover_info[0].get_text().strip(),
                                "turnover": turnover_info[1].get_text().strip()
                            })
                    data_source.append(info_dict)
                except Exception as e:
                    continue
        return data_source

    def get_transaction_info_for_region(self, region):
        candidate_urls = load_cache("chengjiao", region)
        if candidate_urls is None:
            candidate_urls = []
            self.get_candidate_urls(candidate_urls, region)
            print("Total urls", len(candidate_urls))
            save_cache("chengjiao", region, candidate_urls)

        for i, url in enumerate(tqdm(candidate_urls)):
            sale_data_source = self.parse_html(
                html=get_html_content(url), default_info={"region": region})
            with database.atomic():
                if sale_data_source:
                    SellInfoModel.insert_many(sale_data_source).on_conflict_replace().execute()
            save_cache("chengjiao", region, candidate_urls[i + 1:])
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
    while len(region_list) > 0:
        region = region_list.pop(0)
        try:
            # ershoufang_crawler = LianjiaErShouFangCrawler("sh")
            # ershoufang_crawler.get_home_info_for_region(region)
            # zufang_crawler = LianjiaZuFangCrawler("sh")
            # zufang_crawler.get_rent_info_for_region(region)
            xiaoqu_crawler = LianjiaXiaoQuCrawler("sh")
            xiaoqu_crawler.get_community_info_for_region(region)
            # chengjiao_crawler = LianjiaChengJiaoCrawler("sh")
            # chengjiao_crawler.get_transaction_info_for_region(region)
        except:
            region_list.append(region)
            time.sleep(10)
