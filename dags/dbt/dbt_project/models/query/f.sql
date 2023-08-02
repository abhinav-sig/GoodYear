select cast(week_number as String) as week_number, mobile_type,sum(REVENUE_USD)
from {{ref('COMB')}}
group by week_number, mobile_type
union
select 'Overall', mobile_type,sum(REVENUE_USD)
from {{ref('COMB')}}
group by mobile_type
order by mobile_type