select week_number, os_name,min(revenue_usd) min_revenue,max(revenue_usd) max_revenue 
from {{ref('COMB')}}
group by week_number, os_name
order by  os_name,week_number