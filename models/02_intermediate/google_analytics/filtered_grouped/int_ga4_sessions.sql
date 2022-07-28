with daily_session_event_data as(

    select
        date,
        page_device,
        page_hostname,
        count(if(event_name = 'session_start',event_name,null)) as sum_sessions
    from {{ref('stg_ga4_standard_events')}}
    group by 1,2,3
    
)

select * from daily_session_event_data