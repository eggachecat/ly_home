from peewee import *
import datetime
import db.settings as settings

database = MySQLDatabase(
    settings.DB_NAME,
    host=settings.DB_HOST,
    port=settings.DB_PORT,
    user=settings.DB_USER,
    passwd=settings.DB_PASSWORD,
    charset='utf8',
    use_unicode=True,
)


class BaseModel(Model):
    class Meta:
        database = database


class CommunityModel(BaseModel):
    """
        小区
    """
    id = BigIntegerField(primary_key=True)
    title = CharField()
    link = CharField(unique=True)
    district = CharField()
    region = CharField()
    biz_circle = CharField()  # 商圈
    tag_list = CharField()
    on_sale = CharField()
    on_rent = CharField(null=True)
    year = CharField(null=True)
    house_type = CharField(null=True)
    cost = CharField(null=True)
    service = CharField(null=True)
    company = CharField(null=True)
    building_num = CharField(null=True)
    house_num = CharField(null=True)
    price = CharField(null=True)
    city = CharField(null=True)
    valid_date = DateTimeField(default=datetime.datetime.now)

    class Meta:
        table_name = "community"


class HouseInfoModel(BaseModel):
    house_id = CharField(primary_key=True)
    title = CharField()
    link = CharField()
    zone = CharField()
    region = CharField()
    community = CharField()
    years = CharField()
    house_type = CharField()
    square = CharField()
    direction = CharField()
    floor = CharField()
    tax_type = CharField()
    total_price = CharField()
    unit_price = CharField()
    follow_info = CharField()
    decoration = CharField()
    valid_date = DateTimeField(default=datetime.datetime.now)

    class Meta:
        table_name = "house_info"


class HistoricalPriceModel(BaseModel):
    house_id = CharField()
    total_price = CharField()
    date = DateTimeField(default=datetime.datetime.now)

    class Meta:
        primary_key = CompositeKey('house_id', 'total_price')
        table_name = "historical_price"


class SellInfoModel(BaseModel):
    house_id = CharField(primary_key=True)
    title = CharField()
    link = CharField()
    community = CharField()
    years = CharField()
    house_type = CharField()
    square = CharField()
    direction = CharField()
    decoration = CharField()
    turnover = CharField()  # 挂牌到成交时间
    floor = CharField()
    region = CharField()
    tag_list = CharField()
    list_price = CharField()
    total_price = CharField()
    unit_price = CharField()
    deal_date = CharField(null=True)
    update_date = DateTimeField(default=datetime.datetime.now)

    class Meta:
        table_name = "sell_info"


class RentInfoModel(BaseModel):
    house_id = CharField(primary_key=True)
    title = CharField()
    link = CharField()
    region = CharField()
    zone = CharField()
    subway = CharField()
    decoration = CharField()
    price = CharField()
    community = CharField()
    house_type = CharField()
    square = CharField()
    floor = CharField()
    direction = CharField()
    rent_type = CharField()
    update_date = DateTimeField(default=datetime.datetime.now)

    class Meta:
        table_name = "rent_info"


class SubwayCommunityModel(BaseModel):
    community_id = CharField()
    subway_name = CharField()
    subway_stop_name = CharField()
    subway_distance = CharField()

    class Meta:
        primary_key = CompositeKey('community_id', 'subway_name', 'subway_stop_name')
        table_name = "subway_community"


def database_init():
    database.connect()
    database.create_tables(
        [CommunityModel, HouseInfoModel, HistoricalPriceModel, SellInfoModel, RentInfoModel, SubwayCommunityModel],
        safe=True)
    database.close()
