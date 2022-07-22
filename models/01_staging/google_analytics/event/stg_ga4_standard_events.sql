
with ga4_event_params_unnested as (

    select
        parse_date('%Y%m%d', event_date) as date,
        event_timestamp,
        user_pseudo_id,
        event_name,
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
        unnest(event_params) AS event_params
        
    where
    event_name not like('%cart%')
    and event_name not like('%checkout%')
    and event_name not like('%payment%')
    and event_name not like('purchase')
    and event_name not like('%_custom%')

    and _table_suffix not like '%intraday%'
    and PARSE_DATE('%Y%m%d', _table_suffix) between {{ get_last_n_days_date_range(2) }}

), 


ga4_event_params_pivoted as (

    select * 
    from ga4_event_params_unnested
    pivot 
    (
        max(event_value_all_string) as param
        for key in ('page_location',
                    'ga_session_id',
                    'page_referrer'

                )
    )

),

ga4_event_params_surrogate_key as(
    select
        *,
        {{ dbt_utils.surrogate_key([
            'event_timestamp', 
            'param_ga_session_id', 'event_name'
        ])}}  as event_id
    from ga4_event_params_pivoted
)

select * from ga4_event_params_surrogate_key