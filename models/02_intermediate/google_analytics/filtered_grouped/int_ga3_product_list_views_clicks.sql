with product_list_performance as(
SELECT
    date,
    hostname,
    page,
    device,
    productListName,
    productListPosition,
    session_id,
    case when eventAction like ('eec.productImpression')
        then eventAction 
        else null 
    end as list_impression,
    case when eventAction like ('eec.productClick')
        then eventAction 
        else null 
    end as list_click

FROM {{ref('stg_ga3_products')}}
)

select * from product_list_performance