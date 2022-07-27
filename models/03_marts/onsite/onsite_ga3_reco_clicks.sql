
with reco_click_data as(
    SELECT
    date,
    session_id,
    productListName,
    list_impression,
    list_click,
from {{ref('int_ga3_product_list_views_clicks')}}

where productListName = 'recommendation'
    and list_click is not null
),

daily_aggregated_reco_click_data as(
    select
        date,
        count(distinct session_id) as sessions_w_reco_click,
        count(session_id)/count(distinct session_id) as reco_clicks_per_session,
        count(list_click) as total_reco_clicks
    from reco_click_data
    group by 1
    order by date desc
),

daily_all_sessions_data as(
    select
        ga_date,
        count(distinct ga_session_id) as total_sessions
    from {{ref('stg_ga3_session_level_data')}}
    where ga_date between {{ get_last_n_days_date_range(60) }}
    group by 1
    order by 1 desc
),

merged_reco_and_all_sessions_data as(
    select
        * except(ga_date)
    from daily_aggregated_reco_click_data
    left join daily_all_sessions_data on ga_date = date
    order by date desc

)

select * from merged_reco_and_all_sessions_data
