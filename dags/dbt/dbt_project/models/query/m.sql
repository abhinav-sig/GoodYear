select week_number, brand_name,min(revenue_usd) min_revenue,max(revenue_usd) max_revenue 
from {{ref('COMB')}}
group by week_number, brand_name
order by  brand_name,week_number