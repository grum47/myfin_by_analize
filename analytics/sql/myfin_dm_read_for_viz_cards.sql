with bank as
(
    select 'nbrb' as bank_viz
), bank_stat as 
(
    select	date_page 
            , bank_name 
            , price_value_usd_sell
            , lag(price_value_usd_sell, 1) over() as price_value_usd_sell_1_day
            , lag(price_value_usd_sell, 7) over() as price_value_usd_sell_7_day
            , lag(price_value_usd_sell, 30) over() as price_value_usd_sell_30_day
            , lag(price_value_usd_sell, 90) over() as price_value_usd_sell_90_day
            , lag(price_value_usd_sell, 365) over() as price_value_usd_sell_365_day
            , cnt_up 
            , cnt_down 
            , y_predict
    from 	myfin_dm.myfin_by_for_model mfm
    where 	mfm.bank_name = (select bank_viz from bank)
    order by date_page desc 
)
select	*
from 	bank_stat
-- where 	date_page = current_date - '1 day'::interval;