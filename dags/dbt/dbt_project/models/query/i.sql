select cast(week_number as String) as week_number, OS_VENDOR,sum(REVENUE_USD)
from {{ref('COMB')}}
group by week_number, OS_VENDOR
union
select 'Overall', OS_VENDOR,sum(REVENUE_USD)
from {{ref('COMB')}}
group by OS_VENDOR
order by OS_VENDOR