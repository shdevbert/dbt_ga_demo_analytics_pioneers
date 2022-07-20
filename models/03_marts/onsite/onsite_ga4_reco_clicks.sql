with daily_sessions as(
    select
        *
    from {{ref('int_ga4_sessions')}}
),

list_click_data as(
    select
        date,
        sum(list_views) as list_views,
        sum(list_clicks) as list_clicks,
        count(distinct if(list_clicks > 0, param_ga_session_id, null)) as sessions_with_reco_click
    from {{ref('int_ga4_list_views_and_clicks')}}
    where item_list_name = 'recommendation'
    group by 1
),


joined_session_and_reco_data as(
    select *
    from daily_sessions
    left join list_click_data using(date)
)

select * from joined_session_and_reco_data
