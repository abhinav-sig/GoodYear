select cast(week_number as String) week_number, value_segment,sum(REVENUE_USD)
from {{ref('COMB')}}
group by week_number, value_segment
union
select 'Overall', value_segment,sum(REVENUE_USD)
from {{ref('COMB')}}
group by value_segment
order by value_segment,week_number