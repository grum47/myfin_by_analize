-- database PostgreSQL

Create database myfin;

CREATE SCHEMA myfin;


CREATE TABLE myfin.myfin_raw (
	department_id int4 NULL,
	myfin_bank_id int4 NULL,
	myfin_currencies_courses_bank_id int4 NULL,
	price_value_usd_sell numeric NULL,
	price_value_usd_buy numeric NULL,
	price_value_eur_sell numeric NULL,
	price_value_eur_buy numeric NULL,
	price_value_rub_sell numeric NULL,
	price_value_rub_buy numeric NULL,
	date_page date NULL,
	price_value_usd_sell_tm timestamp NULL,
	price_value_usd_buy_tm timestamp NULL,
	price_value_eur_sell_tm timestamp NULL,
	price_value_eur_buy_tm timestamp NULL,
	price_value_rub_sell_tm timestamp NULL,
	price_value_rub_buy_tm timestamp NULL,
	bank_name varchar(50) NULL,
	department_full_address varchar NULL
);
CREATE INDEX myfin_raw_bank_name_idx ON myfin.myfin_raw USING btree (bank_name);
CREATE INDEX myfin_raw_date_page_idx ON myfin.myfin_raw USING btree (date_page);