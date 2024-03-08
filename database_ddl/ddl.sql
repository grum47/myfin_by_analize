-- database PostgreSQL

Create database myfin;

CREATE SCHEMA myfin_raw;
CREATE SCHEMA myfin_dm;

CREATE TABLE myfin_raw.myfin_by (
	myfin_bank_id int4 NULL,
	price_value_usd_sell numeric NULL,
	price_value_usd_buy numeric NULL,
	price_value_eur_sell numeric NULL,
	price_value_eur_buy numeric NULL,
	price_value_rub_sell numeric NULL,
	price_value_rub_buy numeric NULL,
	date_page date NULL,
	bank_name varchar(50) NULL,
);
CREATE INDEX myfin_raw_bank_name_idx ON myfin_raw.myfin_by USING btree (bank_name);
CREATE INDEX myfin_raw_date_page_idx ON myfin_raw.myfin_by USING btree (date_page);

CREATE TABLE myfin_raw.cbr_ru (
	bank_id int4 NOT NULL,
	price_value_usd_sell numeric NULL,
	price_value_usd_buy numeric NULL,
	date_page date NOT NULL,
	bank_name varchar(50) NOT NULL
);
CREATE INDEX cbr_ru_bank_name_idx ON myfin_raw.cbr_ru USING btree (bank_name);
CREATE INDEX cbr_ru_date_page_idx ON myfin_raw.cbr_ru USING btree (date_page);
