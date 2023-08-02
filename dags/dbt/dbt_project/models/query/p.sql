select os_name, 2023-year_of_birth as age
from {{ref('COMB')}}
where 2023-year_of_birth<30 and 2023-year_of_birth>20
group by os_name, year_of_birth
order by  year_of_birth,os_name