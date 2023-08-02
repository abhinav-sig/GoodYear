select cast(week_number as String), 2023-YEAR_OF_BIRTH as YEAR_OF_BIRTH,sum(REVENUE_USD)
from {{ref('COMB')}}
group by week_number, YEAR_OF_BIRTH
union
select 'Overall', 2023-YEAR_OF_BIRTH as YEAR_OF_BIRTH,sum(REVENUE_USD)
from {{ref('COMB')}}
group by YEAR_OF_BIRTH
order by year_of_birth