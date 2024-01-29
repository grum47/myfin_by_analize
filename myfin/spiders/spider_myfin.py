import scrapy
from myfin.items import MyfinItem
from ..spiders import generate_date_list as gdl
from tqdm import tqdm
from datetime import date



class MyFinSpder(scrapy.Spider):

    dt_today = date.today().strftime("%Y-%m-%d")
    dt_list = gdl.check_date_to_db('2024-01-01', dt_today) 

    name = 'myfin'
    allowed_domain = ['myfin.by']

    start_urls = []

    for dt in dt_list:
        start_urls.append(
            f'https://myfin.by/currency/brest/{dt}'
        )
    
    def parse(self, response):
        myfin_item = MyfinItem()

        table_rows = response.xpath('.//*[@class="sort_body"]/tr')
        date_page = response.xpath('.//*[@class="top-content__inline-title"]/h1/text()')

        for row in tqdm(table_rows):
            myfin_item['date_page'] = date_page[1].extract().split()[1].split('.')[2] + '-' + date_page[1].extract().split()[1].split('.')[1] + '-' + date_page[1].extract().split()[1].split('.')[0]
            myfin_item['myfin_bank_id'] = row.xpath('./@id')[0].extract().split('-')[2]
            myfin_item['bank_name'] = row.xpath('./td/span/span/img/@alt')[0].extract()
            myfin_item['price_value_usd_sell'] = row.xpath('./td[@class="currencies-courses__currency-cell"]/span/text()')[0].extract()
            myfin_item['price_value_usd_buy'] = row.xpath('./td[@class="currencies-courses__currency-cell"]/span/text()')[1].extract()
            myfin_item['price_value_eur_sell'] = row.xpath('./td[@class="currencies-courses__currency-cell"]/span/text()')[2].extract()
            myfin_item['price_value_eur_buy'] = row.xpath('./td[@class="currencies-courses__currency-cell"]/span/text()')[3].extract()
            myfin_item['price_value_rub_sell'] = row.xpath('./td[@class="currencies-courses__currency-cell"]/span/text()')[4].extract()
            myfin_item['price_value_rub_buy'] = row.xpath('./td[@class="currencies-courses__currency-cell"]/span/text()')[5].extract()
            
            yield myfin_item
