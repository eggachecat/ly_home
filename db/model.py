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
    biz_circle = CharField()  # 商圈
    tagList = CharField()
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
    floor = CharField()
    status = CharField()
    source = CharField()
    total_price = CharField()
    unitPrice = CharField()
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
    meters = CharField()
    other = CharField()
    subway = CharField()
    decoration = CharField()
    heating = CharField()
    price = CharField()
    price_pre = CharField()
    update_date = DateTimeField(default=datetime.datetime.now)

    class Meta:
        table_name = "rent_info"


def database_init():
    database.connect()
    database.create_tables(
        [CommunityModel, HouseInfoModel, HistoricalPriceModel, SellInfoModel, RentInfoModel], safe=True)
    database.close()
