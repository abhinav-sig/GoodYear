select week_number,
sum(case when gender='MALE' THEN revenue_usd ELSE 0 END) AS male_revenue,
sum(case when gender='FEMALE' THEN revenue_usd ELSE 0 END) AS female_revenue,
sum(case when gender='UNKNOWN' THEN revenue_usd ELSE 0 END) AS unknown_revenue
from {{ref('COMB')}}
group by week_number