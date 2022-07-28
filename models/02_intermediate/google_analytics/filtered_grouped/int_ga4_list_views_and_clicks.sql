with list_page_events as(
    
        date,
        item_list_name,
        page_device,
        page_hostname,
        param_ga_session_id,
        count(if(event_name = 'view_item_list', event_name, null)) as list_views,
        count(if(event_name = 'select_item', event_name, null)) as list_clicks       
    from {{ref('stg_ga4_items')}}
    where 
        event_name in('view_item_list','select_item')
    group by 1,2,3,4,5
    
)

select * from list_page_events