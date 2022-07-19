{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    on_schema_change='fail',
    partition_by={ 'field': 'date', 'data_type': 'date', 'granularity': 'day' }
    ) 
}} 


SELECT
    parse_date('%Y%m%d', date) as date,
    visitid || '_' || fullvisitorid as session_id,
    product.productListName as productListName,
    case when eventInfo.eventAction like ('eec.productImpression')
        then eventInfo.eventAction 
        else null 
    end as reco_impression,
    case when eventInfo.eventAction like ('eec.productClick')
        then eventInfo.eventAction 
        else null 
    end as reco_click,
FROM
    {{ source('ga3_bz_overall', 'ga3_sessions_all') }},
    unnest(hits) as hits,
    unnest(product) as product
where 
    _TABLE_SUFFIX not like '%intraday%'
    and product.productListName = 'recommendation'
    and eventInfo.eventAction in('eec.productClick','eec.productImpression')

{% if is_incremental() %}

    and 
      _TABLE_SUFFIX between FORMAT_DATE('%Y%m%d', _dbt_max_partition) and format_date('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))

      {% else %}

    and 
      PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) between {{ get_last_n_days_date_range(10) }}

{% endif %}

