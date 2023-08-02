
with active_device as (
select count(distinct msisdn) active_devices, week_number from {{ref('COMB')}}
where system_status='ACTIVE'
group by week_number
), all_device as(
select count(distinct msisdn) all_devices, week_number from {{ref('COMB')}}
group by week_number
)
select a.week_number,active_devices,all_devices 
from active_device a inner join all_device b 
on a.week_number=b.week_number
union all
select 'All weeks',sum( distinct active_devices), sum(distinct all_devices) 
from active_device a inner join all_device b 
on a.week_number=b.week_number