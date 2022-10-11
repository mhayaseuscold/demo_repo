Create or replace table PRFT_CUSTOMER_TOTAL_HOURS_130_145 as 
select th.plant_id, customer_id, trim(user) user, qualifier_type, sum(actual_quantity) actual_quantity,
to_char(lock_datetime, 'yyyy-mm-dd') task_date,
 sum(datediff(minutes, lock_datetime, quantity_datetime))/60 total_hours_cust
from TRANSFORM.FACT_TASK_HISTORY th
left join TRANSFORM.DIM_LOCATION_MASTER m
  on th.plant_id = m.plant_id
  and (th.to_location = m.location
  or th.from_location = m.location)
where lock_datetime between dateadd(year, -1, current_date) and dateadd(day, -1 , current_date)
and th.plant_id in (130, 145)
and customer_id <> 0
and datediff(minutes, th.lock_datetime, th.quantity_datetime)/60 between 0 and 24
group by th.plant_id, customer_id, user, qualifier_type, task_date
;
