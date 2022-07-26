{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    on_schema_change='fail',
    partition_by={ 'field': 'date', 'data_type': 'date', 'granularity': 'day' }
    ) 
}}

SELECT
    parse_date('%Y%m%d', date) AS date,
    hits.page.hostname as hostname,
    split(page.pagePath,'?')[offset(0)] as page,
    device.devicecategory as device,
    productSKU,
    v2ProductName,
    v2ProductCategory,
    productBrand,
    productListName,
    productListPosition,
    isclick,
    visitid || '_' || fullvisitorid as session_id,
    eventInfo.eventAction

    FROM
    {{ source('ga3_bz_overall', 'ga3_sessions_all') }},
    UNNEST(hits) AS hits,
    unnest(hits.product) as product

   where _TABLE_SUFFIX not like '%intraday%'
   and _TABLE_SUFFIX > format_date('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY))
   --and product.isclick = true



{% if is_incremental() %}

    and 
      _TABLE_SUFFIX between FORMAT_DATE('%Y%m%d', _dbt_max_partition) and format_date('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))

      {% else %}

    and 
      PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) between {{ get_last_n_days_date_range(60) }}

{% endif %}

   