select cast(week_number as String) as week_number, OS_NAME,COUNT(MSISDN)
from {{ref('COMB')}}
group by week_number, OS_NAME
union
select 'Overall', OS_NAME,COUNT(MSISDN)
from {{ref('COMB')}}
group by OS_NAME
order by OS_NAME