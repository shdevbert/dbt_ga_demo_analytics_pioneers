{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    on_schema_change='fail',
    partition_by={ 'field': 'date', 'data_type': 'date', 'granularity': 'day' }
    ) 
}} 


with ga4_event_params_unnested as (

    select
        parse_date('%Y%m%d', event_date) as date,
        item_id,
        item_name,
        item_brand,
        item_category,
        item_list_name,
        price as item_price,
        event_name,
        event_timestamp,
        user_pseudo_id,
        device.category as page_device,
        device.web_info.hostname as page_hostname,
        event_params.key as key,
        
        /* recast all event values to string and select the first non-null value, to combine all in one column */
        coalesce(
            value.string_value,
            cast(value.int_value as string),
            cast(value.float_value as string),
            cast(value.double_value as string)
        ) as event_value_all_string
        
    from
        {{ source('ga4_bz_overall', 'ga4_events_all') }},
        unnest(event_params) AS event_params,
        unnest(items) as items
        
    where 
    _table_suffix not like '%intraday%'

    {% if is_incremental() %}
        and 
        _table_suffix between FORMAT_DATE('%Y%m%d', _dbt_max_partition) and format_date('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        {% else %}
        and 
        PARSE_DATE('%Y%m%d', _table_suffix) between {{ get_last_n_days_date_range(2) }}
    {% endif %}

), 


ga4_event_params_pivoted as (

    select * 
    from ga4_event_params_unnested
    pivot 
    (
        max(event_value_all_string) as param
        for key in ('page_location',
                    'ga_session_id'

                )
    )

)

select * from ga4_event_params_pivoted