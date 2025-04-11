import pandas as pd

from . import Filter
from .db import Postgres


def build_query(config: dict) -> str:

    return f"""
    with close_prices as (SELECT trade_date, asset_code,
    (case when (nyse_close_price is NULL) or (nyse_close_price = 0) then close_price 
    when (nyse_close_price is NULL) or (nyse_close_price = 0) then eve_lastdeal_price else nyse_close_price end) as close_price
    from "ARH_TradeResult" 
    where trade_date {f'''between ('{config["date_start"]}') and ('{config["date_finish"]}')'''}
    and trade_mode in ('Режим основных торгов') 
    and section_code in ('EQF', 'EBOND', 'EQCIS', 'EQR')),

    raw_data as (select tr.trade_date, tr.asset_code, currency, trade_member_name, account, client_code, client_legal_code,
    count(distinct contra_client_code) as contra_clients_qty, -- count of contra clients
    count(distinct tr.asset_code) as traded_assets, 
    count(distinct deal_id) as deals_qty, 
    count(distinct deal_id)::numeric / count(distinct contra_client_code)::numeric as avg_deals_per_contra, -- average of deals per contra client
    count(distinct order_id) as orders_qty,
    count(distinct order_id)::numeric / count(distinct contra_client_code)::numeric as avg_orders_per_contra, -- average of orders per contra client
    count(distinct order_id)::numeric / count(distinct tr.asset_code)::numeric as avg_orders_per_asset, -- average of orders per asset
    min(deal_time) as min_deal_time,
    max(deal_time) as max_deal_time,
    extract ('epoch' from (max(deal_time) - min(deal_time))) as deal_time_delta,
    extract ('epoch' from (max(deal_time) - min(deal_time))) / count(distinct order_id) as avg_time_btw_orders_secs,
    sum(trade_value) as vol_money,
    sum(trade_value) / count(distinct contra_client_code) as avg_vol_per_contra, -- average volume per contra client
    sum(trade_qty) as vol_lots,
    sum(trade_value * trade_qty) / sum(trade_qty) as avg_deal_vol, -- average deal value
    max(trade_value) as max_deal_vol, -- max deal value
    sum(price * trade_qty) / sum(trade_qty) as avg_deal_price,

    sum(case when dir = '1' then trade_qty * cprs.close_price else trade_qty * -1 * cprs.close_price end) + sum(case when dir = '2' then trade_value else trade_value * -1 end) as fin_res,

    ABS(sum(case when dir = '1' then trade_qty * cprs.close_price else trade_qty * -1 * cprs.close_price end) + sum(case when dir = '2' then trade_value else trade_value * -1 end)) as fin_res_abs,

    sum(case when is_extra_liqudity = '1' and dir = '1' then trade_qty * cprs.close_price when is_extra_liqudity = '1' and dir = '2' then trade_qty * -1 * cprs.close_price end) 
    + sum(case when is_extra_liqudity = '1' and dir = '2' then trade_value when is_extra_liqudity = '1' and dir = '1' then trade_value * -1 end) as fin_res_ext,

    ABS(sum(case when is_extra_liqudity = '1' and dir = '1' then trade_qty * cprs.close_price when is_extra_liqudity = '1' and dir = '2' then trade_qty * -1 * cprs.close_price end) 
     + sum(case when is_extra_liqudity = '1' and dir = '2' then trade_value when is_extra_liqudity = '1' and dir = '1' then trade_value * -1 end)) as fin_res_ext_abs,

    least(sum(case when is_extra_liqudity = '0' and dir = '1' then trade_qty * cprs.close_price when is_extra_liqudity = '0' and dir = '2' then trade_qty * -1 * cprs.close_price end) 
    + sum(case when is_extra_liqudity = '0' and dir = '2' then trade_value when is_extra_liqudity = '0' and dir = '1' then trade_value * -1 end))  as fin_res_int,

    ABS(sum(case when is_extra_liqudity = '0' and dir = '1' then trade_qty * cprs.close_price when is_extra_liqudity = '0' and dir = '2' then trade_qty * -1 * cprs.close_price end) 
    + sum(case when is_extra_liqudity = '0' and dir = '2' then trade_value when is_extra_liqudity = '0' and dir = '1' then trade_value * -1 end)) as fin_res_int_abs,

    sum(case 
    when currency != 'RUB' and price >= 10 and is_extra_liqudity = '1' then 0.0001 * abs(trade_qty)
    when currency != 'RUB' and price < 10 and is_extra_liqudity = '1' then 0.0012 * abs(trade_qty)
    when currency = 'RUB' and is_extra_liqudity = '1' then 0.1 * abs(trade_qty) else 0 end) as "Cost"



    from "vw_ARH_TradeBS" tr
    left join close_prices cprs on cprs.trade_date = tr.trade_date and cprs.asset_code = tr.asset_code 


        where tr.trade_date {f'''between ('{config["date_start"]}') and ('{config["date_finish"]}')'''}
        and trade_mode in ('Режим основных торгов')
        and currency in ('{config["currency"]}')
        and client_code not in ({",".join(map(lambda x: f"'{x}'", config["exclude_codes"]))})

        group by tr.trade_date, tr.asset_code, currency, trade_member_name, account, client_code, client_legal_code)

        select * from raw_data
    """


async def get_unusual_deals(config: dict, db: Postgres)->pd.DataFrame:
    raw_data = await db.fetch(build_query(config))
    df = pd.DataFrame([dict(r) for r in raw_data])
    return df

def apply_filter_chain(df: pd.DataFrame, filters: list[Filter])->pd.DataFrame:
    res_df = df
    for f in filters: res_df = f.apply(res_df)
    return res_df
