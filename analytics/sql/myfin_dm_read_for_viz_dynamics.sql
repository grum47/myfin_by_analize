with bank as
(
    select 'nbrb' as bank_viz
)
select	date_page::date as date_page 
        , bank_name 
        , price_value_usd_sell 
        , mean_14_price_usd_sell 
        , mean_28_price_usd_sell 
        , case when mean_14_price_usd_sell > mean_28_price_usd_sell then 1 else 0 end as is_14_above_28
        , case when mean_28_price_usd_sell > mean_14_price_usd_sell then 1 else 0 end as is_28_above_14
        , abs((mean_14_price_usd_sell - mean_28_price_usd_sell) / price_value_usd_sell * 100) as abs_distance_btw_14_28
        , case when mean_14_price_usd_sell - lag(mean_14_price_usd_sell, 1) over() > 0 then 1 else 0 end as is_14_up
        , case when mean_28_price_usd_sell - lag(mean_28_price_usd_sell, 1) over() > 0 then 1 else 0 end as is_28_up
from 	myfin_dm.myfin_by_for_model mfm 
where 	mfm.bank_name = (select bank_viz from bank)
and 	date_page >= current_date - '1 year'::interval
order by date_page;