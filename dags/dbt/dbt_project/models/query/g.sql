select cast(week_number as String) as week_number, BRAND_NAME,sum(REVENUE_USD)
from {{ref('COMB')}}
group by week_number, BRAND_NAME
union
select 'Overall', BRAND_NAME,sum(REVENUE_USD)
from {{ref('COMB')}}
group by BRAND_NAME
order by BRAND_NAME