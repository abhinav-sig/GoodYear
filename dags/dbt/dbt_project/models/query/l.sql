select cast(week_number as String) as week_number, MOBILE_TYPE,COUNT(MSISDN)
from {{ref('COMB')}}
group by week_number, MOBILE_TYPE
union
select 'Overall', MOBILE_TYPE,COUNT(MSISDN)
from {{ref('COMB')}}
group by MOBILE_TYPE
order by MOBILE_TYPE