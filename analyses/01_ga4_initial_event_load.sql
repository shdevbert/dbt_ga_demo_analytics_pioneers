with ga4_initial_event_load as (

    select
        parse_date('%Y%m%d', event_date) as date,
        event_timestamp,
        user_pseudo_id,
        event_name,
        device.category as page_device,
        device.web_info.hostname as page_hostname,
        event_params
        
    from
        {{ source('ga4_bz_overall', 'ga4_events_all') }}
        
    where
    event_name in('select_item')

    and _table_suffix not like '%intraday%'
    and PARSE_DATE('%Y%m%d', _table_suffix) between {{ get_last_n_days_date_range(2) }}

)

select * from ga4_initial_event_load