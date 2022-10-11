Create or replace table CDW.TRANSFORM.PRFT_CUSTOMER_TOTAL_HOURS_130_145 as
select th.plant_id, customer_id, trim(user) user, from_location, to_location, count(license) pallet_count, qualifier_type, sum(actual_quantity) actual_quantity,
to_char(lock_datetime, 'yyyy-mm-dd') task_date,
 sum(datediff(minutes, lock_datetime, quantity_datetime))/60 total_hours_cust
from CDW.TRANSFORM.FACT_TASK_HISTORY th
left join CDW.TRANSFORM.DIM_LOCATION_MASTER m
  on th.plant_id = m.plant_id
  and (th.to_location = m.location
  or th.from_location = m.location)
where lock_datetime between dateadd(year, -1, current_date) and dateadd(day, -1 , current_date)
and th.plant_id in (130, 145)
and th.qualifier_type in ('BK', 'PL', 'LP')
and customer_id <> 0
and datediff(minutes, th.lock_datetime, th.quantity_datetime)/60 between 0 and 24
group by th.plant_id, customer_id, user, from_location, to_location, qualifier_type, task_date
union
 select th.plant_id, customer_id, trim(user) user, from_location, to_location, count(license) pallet_count, qualifier_type, sum(actual_quantity) actual_quantity,
to_char(lock_datetime, 'yyyy-mm-dd') task_date,
sum(datediff(minutes, lock_datetime, quantity_datetime))/60 total_hours_cust 
from CDW.TRANSFORM.FACT_TASK_HISTORY th
left join CDW.TRANSFORM.DIM_LOCATION_MASTER m
on th.plant_id = m.plant_id
and (th.to_location = m.location
or th.from_location = m.location)
where lock_datetime between dateadd(year, -1, current_date) and dateadd(day, -1 , current_date)
and th.plant_id in (130)
and customer_id <> 0
and user like '%AUTOLOC%'
group by th.plant_id, customer_id, user, from_location, to_location, qualifier_type, task_date
union 
select th.plant_id, customer_id, trim(user) user, from_location, to_location, count(license) pallet_count, qualifier_type, sum(actual_quantity) actual_quantity,
to_char(lock_datetime, 'yyyy-mm-dd') task_date,
sum(datediff(minutes, lock_datetime, quantity_datetime))/60 total_hours_cust
from CDW.TRANSFORM.FACT_TASK_HISTORY th
left join CDW.TRANSFORM.DIM_LOCATION_MASTER m
on th.plant_id = m.plant_id
and (th.to_location = m.location
or th.from_location = m.location)
where lock_datetime between dateadd(year, -1, current_date) and dateadd(day, -1 , current_date)
and th.plant_id in (145)
and customer_id <> 0
and user like '%AUTOLOC%'
group by th.plant_id, customer_id, user, from_location, to_location, qualifier_type, task_date
union
select plant_id, customer_id, user, 'Blank' as from_location, to_location, pallet_count, qualifier_type, actual_quantity, task_date,
total_hours_cust
from(
select plant_id,
customer_ID,
trim(last_maintenance_program) as user,
case when qualifier  = '01' then 'PW'
else null end as Qualifier_type,
location_ID as to_location,
date(LAST_MAINTENANCE_DATETIME) as task_date,
sum(location_quantity) as Actual_Quantity,
count(distinct LICENSE_ID) as Pallet_count,
null as Total_Hours_Cust,
null as Adj_Total_Hours_Cust
from transform.facT_location_history
where plant_ID in ('145')  and qualifier in('01') and last_maintenance_datetime between dateadd(year, -1, current_date) and current_date - 1
and task_date between dateadd(year, -1, current_date) and dateadd(day, -1 , current_date)
group by plant_ID, customer_Id, last_maintenance_program,qualifier,location_ID, date(LAST_MAINTENANCE_DATETIME)
  )
;
