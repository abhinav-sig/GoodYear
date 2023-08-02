select cast(week_number as String) as week_number, OS_NAME,sum(REVENUE_USD)
from {{ref('COMB')}}
group by week_number, OS_NAME
union
select 'Overall', OS_NAME,sum(REVENUE_USD)
from {{ref('COMB')}}
group by OS_NAME
order by OS_NAME