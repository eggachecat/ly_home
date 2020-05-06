import time

from bs4 import BeautifulSoup
from tqdm import tqdm

from db.model import database, HouseInfoModel, HistoricalPriceModel, database_init
from lianjia.utils import get_html_content, logger

ER_SHOU_FANG_PRICE_FILTERS = [f"p{i}" for i in range(1, 8)]
ER_SHOU_FANG_ROOM_FILTERS = [f"l{i}" for i in range(1, 7)]

ER_SHOU_FANG_FILTERS = [
    ER_SHOU_FANG_PRICE_FILTERS,
    ER_SHOU_FANG_ROOM_FILTERS
]

ZU_FANG_RENT_PRICE_FILTERS = [f"rp{i}" for i in range(1, 8)]


def make_url(base_url, region, parameters):
    return base_url + "{region}/{parameters}/".format(
        region=region, parameters="".join(parameters)
    )


class BaseCrawler:
    """
            抽象了一些公共方法
    """

    def __init__(self, base_url):
        self.base_url = base_url

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
        if number_of_pages >= 100 and filter_level < len(ER_SHOU_FANG_FILTERS):
            for f in ER_SHOU_FANG_FILTERS[filter_level]:
                self.get_candidate_urls(urls, region, filters + [f], filter_level + 1)
        else:
            if filter_level >= len(ER_SHOU_FANG_FILTERS):
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
        super().__init__(f"http://{city}.lianjia.com/")
        self.city = city

    def get_number_of_pages(self, soup):
        try:
            page_info = soup.find('div', {'class': 'page-box house-lst-page-box'})
            page_info_str = page_info.get('page-data').split(',')[0]
            total_pages = int(page_info_str.split(':')[1])
            return total_pages
        except:
            return 0

    def parse_html(self, html):
        house_info_data_source = []
        historical_price_data_source = []
        soup = BeautifulSoup(html, 'lxml')
        for ul_tag in soup.findAll("ul", {"class": "sellListContent"}):
            for name in ul_tag.find_all('li'):
                info_dict = {}
                try:
                    house_title = name.find("div", {"class": "title"})
                    info_dict.update(
                        {'title': house_title.a.get_text().strip()})
                    info_dict.update({'link': house_title.a.get('href')})
                    house_id = house_title.a.get('data-housecode')
                    info_dict.update({'house_id': house_id})

                    house_info = name.find("div", {"class": "houseInfo"})
                    info = house_info.get_text().split('|')
                    # info_dict.update({'community': info[0]})
                    info_dict.update({'house_type': info[0]})
                    info_dict.update({'square': info[1]})
                    info_dict.update({'direction': info[2]})
                    info_dict.update({'decoration': info[3]})
                    info_dict.update({'floor': info[4]})
                    info_dict.update({'years': info[5]})

                    house_floor = name.find("div", {"class": "positionInfo"})
                    community_info = house_floor.get_text().split('-')
                    info_dict.update({'community': community_info[0]})

                    follow_info = name.find("div", {"class": "followInfo"})
                    info_dict.update(
                        {'follow_info': follow_info.get_text().strip()})

                    tax_free = name.find("span", {"class": "taxfree"})
                    if tax_free is None:
                        info_dict.update({"tax_type": ""})
                    else:
                        info_dict.update(
                            {"tax_type": tax_free.get_text().strip()})

                    total_price = name.find("div", {"class": "totalPrice"})
                    info_dict.update(
                        {'total_price': total_price.span.get_text()})

                    unit_price = name.find("div", {"class": "unitPrice"})
                    info_dict.update(
                        {'unit_price': unit_price.get("data-price")})
                except:
                    continue

                house_info_data_source.append(info_dict)
                historical_price_data_source.append(
                    {"house_id": info_dict["house_id"], "total_price": info_dict["total_price"]})

        return house_info_data_source, historical_price_data_source

    def get_home_info_for_region(self, region, with_url_cache=True):
        """
            对于每个区的二手房的爬虫
        """
        candidate_urls = []
        if with_url_cache:
            with open("../.cache/url_candidates", "r") as fp:
                candidate_urls = [line.strip() for line in fp.readlines()]
        else:
            self.get_candidate_urls(candidate_urls, region)
            with open("../.cache/url_candidates", "w") as fp:
                fp.write("\n".join(candidate_urls))

        for url in tqdm(candidate_urls):
            house_info_data_source, historical_price_data_source = self.parse_html(html=get_html_content(url))
            with database.atomic():
                if house_info_data_source:
                    HouseInfoModel.insert_many(house_info_data_source).on_conflict_replace().execute()
                if historical_price_data_source:
                    HistoricalPriceModel.insert_many(
                        historical_price_data_source).on_conflict_replace().execute()
            time.sleep(1)


class LianjiaZuFangCrawler(BaseCrawler):
    """
        租房的爬虫
        现在也是从列表来
    """

    def __init__(self, city, base_url):
        super().__init__(base_url)
        self.city = city

    def get_number_of_pages(self, soup):
        try:
            page_navigation = soup.find('div', {'data-el': 'page_navigation'})
            return int(page_navigation.get('data-totalpage'))
        except:
            return 0

    def get_rent_info_for_region(self, region):
        pass


if __name__ == '__main__':
    database_init()
    ershoufang_crawler = LianjiaErShouFangCrawler("sh")
    ershoufang_crawler.get_home_info_for_region("pudong", with_url_cache=False)
