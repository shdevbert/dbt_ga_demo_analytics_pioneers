{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    on_schema_change='fail',
    partition_by={ 'field': 'ga_date', 'data_type': 'date', 'granularity': 'day' }
    ) 
}} 

--fetch  data

select 

  parse_date('%Y%m%d', date) as ga_date,
  visitid || '_' || fullvisitorid as ga_session_id,
  hits.page.hostname as ga_hostname,
  channelgrouping as ga_acquisition_channel,

  case
    When channelGrouping IN ("Brand Paid Search", "Organic Search Home", "Direct") THEN "Brand"
    When channelGrouping IN ("Generic Paid Search Google", "Display", "Generic Paid Search Bing", "PSM", "Affiliate", "Social Paid", "Native") THEN "Paid"
    When channelGrouping IN ("Organic Search Non-Home", "Referral", "Hersteller Links", "Social Organic", "Organic Search Magazin") THEN "Organic"
    When channelGrouping IN ("Newsletter", "Triggermail") THEN "Email"
  else "Other"
  end  as ga_acquisition_channel_groups,

  trafficsource.campaign as ga_campaign,
  device.devicecategory as ga_device_category,
  hits.item.transactionid as ga_transaction_id
  
from 
    {{ source('ga3_bz_overall', 'ga3_sessions_all') }},
    unnest(hits) as hits

where 
    _TABLE_SUFFIX not like '%intraday%'

{% if is_incremental() %}

    and 
      _TABLE_SUFFIX between FORMAT_DATE('%Y%m%d', _dbt_max_partition) and format_date('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))

      {% else %}

    and 
      PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) between {{ get_last_n_days_date_range(90) }}

{% endif %}
    
group by 1,2,3,4,5,6,7,8