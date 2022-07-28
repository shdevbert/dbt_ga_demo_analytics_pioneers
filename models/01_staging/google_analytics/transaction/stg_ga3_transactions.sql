select

  parse_date('%Y%m%d',  date) as ga_date,
  visitid || '_' || fullvisitorid as ga_session_id,
  fullvisitorid as ga_fullvisitor_id,
  channelgrouping as ga_acquisition_channel,
  trafficsource.campaign as ga_campaign,
  device.devicecategory as ga_device_category,
  hits.transaction.transactionid as ga_transaction_id,
  round(hits.transaction.transactionrevenue / 1000000,3) as ga_transaction_order_value,
  round(hits.transaction.transactiontax / 1000000,3) as ga_transaction_tax,
  round(hits.transaction.transactionshipping / 1000000,3) as ga_transaction_shipping

from
  {{ source('ga3_bz_overall', 'ga3_sessions_all') }},
  unnest(hits) as hits
  
where _TABLE_SUFFIX not like '%intraday%'
  and PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) between date_sub(current_date(), interval 1 day) and date_sub(current_date(), interval 1 day)
  and hits.transaction.transactionid is not null

group by 1,2,3,4,5,6,7,8,9,10