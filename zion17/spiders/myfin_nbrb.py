import scrapy
from zion17.items import MyfinItem
from . import generate_date_list as gdl
from datetime import date



class MyFinNbrbSpider(scrapy.Spider):

    dt_today = date.today().strftime("%Y-%m-%d")
    dt_list = gdl.check_date_to_db_nbrb('2019-01-01', dt_today) 

    name = 'nbrb'
    allowed_domain = ['myfin.by']

    start_urls = []

    for dt in dt_list:
        start_urls.append(
            f'https://myfin.by/currency/brest/{dt}'
        )


    def parse(self, response):
        myfin_item = MyfinItem()

        date_page = response.xpath('.//*[@class="top-content__inline-title"]/h1/text()')        

        myfin_item['date_page'] = date_page[1].extract().split()[1].split('.')[2] + '-' + date_page[1].extract().split()[1].split('.')[1] + '-' + date_page[1].extract().split()[1].split('.')[0]
        myfin_item['myfin_bank_id'] = 999999
        myfin_item['bank_name'] = 'НБРБ'
        myfin_item['price_value_usd_sell'] = response.xpath('.//*[@class="course-brief-info course-brief-info--nbrb course-brief-info--desk"]/div[2]/div[2]/div[1]/span/text()')[0].extract()
        myfin_item['price_value_usd_buy'] = response.xpath('.//*[@class="course-brief-info course-brief-info--nbrb course-brief-info--desk"]/div[2]/div[2]/div[1]/span/text()')[0].extract()
        myfin_item['price_value_eur_sell'] = response.xpath('.//*[@class="course-brief-info course-brief-info--nbrb course-brief-info--desk"]/div[2]/div[4]/div[1]/span/text()')[0].extract()
        myfin_item['price_value_eur_buy'] = response.xpath('.//*[@class="course-brief-info course-brief-info--nbrb course-brief-info--desk"]/div[2]/div[4]/div[1]/span/text()')[0].extract()
        myfin_item['price_value_rub_sell'] = response.xpath('.//*[@class="course-brief-info course-brief-info--nbrb course-brief-info--desk"]/div[2]/div[6]/div[1]/span/text()')[0].extract()
        myfin_item['price_value_rub_buy'] = response.xpath('.//*[@class="course-brief-info course-brief-info--nbrb course-brief-info--desk"]/div[2]/div[6]/div[1]/span/text()')[0].extract()

        yield myfin_item
