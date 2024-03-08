# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class MyfinItem(scrapy.Item):

    date_page = scrapy.Field()
    bank_name = scrapy.Field()
    myfin_bank_id = scrapy.Field()
    price_value_usd_sell = scrapy.Field()
    price_value_usd_buy = scrapy.Field()
    price_value_eur_sell = scrapy.Field()
    price_value_eur_buy = scrapy.Field()
    price_value_rub_sell = scrapy.Field()
    price_value_rub_buy = scrapy.Field()
    
