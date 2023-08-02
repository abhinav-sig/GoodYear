select brand_name, 2023-year_of_birth AS age
from {{ref('COMB')}}
where 2023-year_of_birth<30 and 2023-year_of_birth>20
group by brand_name, year_of_birth
order by  year_of_birth,brand_name