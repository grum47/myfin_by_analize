# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter

import psycopg2
from omegaconf import OmegaConf

conf = OmegaConf.load('conf/server/db/postgres.yaml')

class MyfinPipeline:

    def __init__(self):
        self.connection = psycopg2.connect(
            host=conf.postgres.host,
            port=conf.postgres.port,
            dbname=conf.postgres.dbname,
            user=conf.postgres.user,
            password=conf.postgres.password
        )

        self.cursor = self.connection.cursor()

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS myfin_raw.myfin_by (
                    myfin_bank_id int4 NULL,
                    price_value_usd_sell numeric NULL,
                    price_value_usd_buy numeric NULL,
                    price_value_eur_sell numeric NULL,
                    price_value_eur_buy numeric NULL,
                    price_value_rub_buy numeric NULL,
                    date_page date NULL,
                    bank_name varchar(50) NULL
                );
        """)
        # self.connection.commit()



    def process_item(self, item, spider):

        self.connection = psycopg2.connect(
            host=conf.postgres.host,
            port=conf.postgres.port,
            dbname=conf.postgres.dbname,
            user=conf.postgres.user,
            password=conf.postgres.password
        )

        self.cursor = self.connection.cursor()
        
        try:
            query = """
                    insert into myfin_raw.myfin_by (
                    myfin_bank_id,
                    price_value_usd_sell,
                    price_value_usd_buy,
                    price_value_eur_sell,
                    price_value_eur_buy,
                    price_value_rub_sell,
                    price_value_rub_buy,
                    date_page,
                    bank_name
                    ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

            self.cursor.execute(query, (
                item["myfin_bank_id"],
                item["price_value_usd_sell"],
                item["price_value_usd_buy"],
                item["price_value_eur_sell"],
                item["price_value_eur_buy"],
                item["price_value_rub_sell"],
                item["price_value_rub_buy"],
                item["date_page"],
                item["bank_name"]
                ))
            
            self.connection.commit()
            self.connection.close()
        except:
            self.connection.close()
        
        return item
    
    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()
