select  
    date_page
    , bank_name
    , price_value_usd_sell
    , price_value_usd_buy - price_value_usd_sell as bank_spred
from 	myfin_raw.myfin_by mr;