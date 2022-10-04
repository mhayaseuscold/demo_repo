DELETE FROM TRANSFORM.PRFT_CUSTOMER_TOTAL_HOURS
WHERE YEAR=RUN_YEAR AND MONTH=RUN_MONTH;

insert into TRANSFORM.PRFT_CUSTOMER_TOTAL_HOURS
select th.plant_id, customer_id, trim(user) user,
month(lock_datetime) month, year(lock_datetime) year, 
 sum(datediff(minutes, lock_datetime, quantity_datetime))/60 total_hours_cust
from TRANSFORM.FACT_TASK_HISTORY th
left join TRANSFORM.DIM_LOCATION_MASTER m
  on th.plant_id = m.plant_id
  and (th.to_location = m.location
  or th.from_location = m.location)
where year(lock_datetime)=RUN_YEAR and month(lock_datetime)=RUN_MONTH
and customer_id <> 0
and th.qualifier_type not in ('RO', 'PA', 'RA')
and m.qualifier not in ('BL')
and trim(th.user) not in ('AUTOLOC', 'System')
and datediff(minutes, th.lock_datetime, th.quantity_datetime)/60 between 0 and 24
group by th.plant_id, customer_id, user, month, year;


MERGE INTO TRANSFORM.PRFT_CUSTOMER_TOTAL_HOURS TGT
USING 
(
WITH TASK_INTERSECTION AS (
select 
plant_id, customer_id, ticket_id, task_id, qualifier_type, trim(user) user,
license, lock_date, lock_time, lock_datetime, quantity_date, quantity_time, quantity_datetime,
sum(datediff(minutes, lock_datetime, quantity_datetime))/60 total_time_user
from TRANSFORM.FACT_TASK_HISTORY
where qualifier_type in ('BP', 'RO', 'PL') 
and trim(user) not in ('AUTOLOC', 'System')
and datediff(minutes, lock_datetime, quantity_datetime)/60 between 0 and 24
and month(lock_datetime) = RUN_MONTH
and year(lock_datetime) = RUN_YEAR
group by plant_id, customer_id, ticket_id, task_id, qualifier_type, user,
license, lock_date, lock_time, lock_datetime, quantity_date, quantity_time, quantity_datetime
), BREAK_TASK_HOURS as (
  select ti.plant_id, ti.customer_id br_customer_id, ti.task_id, ti.qualifier_type, trim(ti.user) user,
ti.lock_date break_startdate, ti.lock_time break_starttime, ti.lock_datetime break_start_datetime, 
ti.quantity_datetime break_end_datetime,
sum(datediff(minutes, ti.lock_datetime, ti.quantity_datetime))/60 br_total_time_user
from TRANSFORM.FACT_TASK_HISTORY  ti
where ti.qualifier_type in ('BR', 'LU')
and month(ti.lock_datetime) = RUN_MONTH
and year(ti.lock_datetime) = RUN_YEAR
and trim(ti.user) not in ('AUTOLOC', 'System')
and datediff(minutes, ti.lock_datetime, ti.quantity_datetime)/60 between 0 and 24
group by  ti.plant_id, ti.customer_id, ti.task_id, ti.qualifier_type, ti.user,
ti.lock_date, ti.lock_time, ti.lock_datetime, 
ti.quantity_date, ti.lock_datetime, ti.quantity_datetime
)  , BREAK_HOUR_INTERSECTION as (
select plant_id, customer_id, user, month, year, sum(br_intersect_time_user) br_intersect_time_cust from (
select bt.plant_id, ti.customer_id, ti.user, ti.task_id, bt.task_id break_task_id,  ti.qualifier_type, bt.qualifier_type break_qualifier_type,
ti.lock_datetime, ti.quantity_datetime, bt.break_start_datetime, bt.break_end_datetime, month(lock_datetime) month, year(lock_datetime) year,
sum(datediff(minutes, bt.break_start_datetime, bt.break_end_datetime))/60 br_intersect_time_user,
case when bt.break_start_datetime between ti.lock_datetime and ti.quantity_datetime
and bt.break_end_datetime between ti.lock_datetime and ti.quantity_datetime
then 1
else 2
end as break_intersection
from BREAK_TASK_HOURS bt 
left join TASK_INTERSECTION  ti
on ti.plant_id = bt.plant_id 
and trim(ti.user) = trim(bt.user)
where bt.qualifier_type in ('BR', 'LU')
  and month(ti.lock_datetime) = RUN_MONTH
and year(ti.lock_datetime) = RUN_YEAR
and break_intersection = 1
group by  bt.plant_id, ti.customer_id, ti.user, ti.task_id, bt.task_id, ti.qualifier_type, bt.qualifier_type,
ti.lock_datetime, ti.quantity_datetime, bt.break_start_datetime, bt.break_end_datetime
)
  group by plant_id, customer_id, user, month, year
)                                                                                                                                                                                                                                               
select bh.plant_id, bh.customer_id, bh.user, cp.month, cp.year, cp.total_hours_cust, cp.total_hours_cust - sum(bh.br_intersect_time_cust) ADJ_TOTAL_HOURS_CUST
from BREAK_HOUR_INTERSECTION bh
left join (select plant_id, customer_id, user, month, year, sum(total_hours_cust) total_hours_cust from TRANSFORM.PRFT_CUSTOMER_TOTAL_HOURS
          group by plant_id, customer_id, user, month, year) cp
on bh.plant_id = cp.plant_id
and bh.customer_id = cp.customer_id
and bh.user = cp.user
and bh.month = cp.month
and bh.year = cp.year
where cp.month = RUN_MONTH
and cp.year = RUN_YEAR
group by bh.plant_id, bh.customer_id, bh.user, cp.month, cp.year, cp.total_hours_cust 
) STG
ON TGT.PLANT_ID = STG.PLANT_ID
AND TGT.CUSTOMER_ID = STG.CUSTOMER_ID
AND TGT.USER = STG.USER
AND TGT.MONTH = STG.MONTH
AND TGT.YEAR = STG.YEAR
WHEN MATCHED THEN
UPDATE
SET TGT.TOTAL_HOURS_CUST = STG.ADJ_TOTAL_HOURS_CUST
;

DELETE FROM TRANSFORM.PRFT_CUST_CASE_PICK_HOURS 
WHERE YEAR=RUN_YEAR AND MONTH=RUN_MONTH;

insert into TRANSFORM.PRFT_CUST_CASE_PICK_HOURS 
WITH DOUBLE_STACK AS 
(
select th.plant_id, th.customer_id, trim(th.user) user, th.task_id, td.month, td.year, min(tdh.time_diff) time_diff
from TRANSFORM.FACT_TASK_HISTORY th 
inner join (select plant_id, customer_id, user_id, task_id, lock_date, month, year 
  from (
  select d.plant_id, d.customer_id, d.user_id, d.task_id, d.cube, d.sequence_number, d.inbound_license, d.rrn, 
    d.product_id, d.lock_date, month(d.lock_datetime) month, year(d.lock_datetime) year, sum(datediff(minutes, d.lock_datetime, d.quantity_datetime))/60 time_diff 
            from TRANSFORM.FACT_TASK_HISTORY_DETAILS d
            left join TRANSFORM.DIM_PRODUCT_MASTER p
            on p.customer_id = d.customer_id
            and p.product_id = d.product_id
           where month = RUN_MONTH and year = RUN_YEAR and d.qualifier_id = 'BP' 
            and d.quantity = p.pallet_factor
            and trim(d.user_id) not in ('AUTOLOC', 'System')
            and datediff(minutes, d.lock_datetime, d.quantity_datetime)/60 between 0 and 24
           group by d.plant_id, d.customer_id, d.user_id, d.task_id, d.cube, d.sequence_number, d.inbound_license, d.rrn, 
    d.product_id, d.lock_date, d.lock_datetime  
   ) 
  group by plant_id, customer_id, user_id, task_id, lock_date, month, year
  having count(*) = 2
   ) td            
on th.plant_id = td.plant_id
and th.task_id = td.task_id
and th.customer_id = td.customer_id
and trim(th.user) = trim(td.user_id)
and month(th.lock_datetime) = td.month
and year(th.lock_datetime) = td.year
    left join 
       (select d.plant_id, d.customer_id, d.user_id, d.task_id, d.cube, d.sequence_number, d.inbound_license, d.rrn, 
      d.product_id, d.lock_date, 
       month(d.lock_datetime) month, year(d.lock_datetime) year, sum(datediff(minutes, d.lock_datetime, d.quantity_datetime))/60 time_diff
              from TRANSFORM.FACT_TASK_HISTORY_DETAILS d
                left join TRANSFORM.DIM_PRODUCT_MASTER p
                on p.customer_id = d.customer_id
                and p.product_id = d.product_id
               where month = RUN_MONTH and year = RUN_YEAR and d.qualifier_id = 'BP'
                and d.quantity = p.pallet_factor
                and trim(d.user_id) not in ('AUTOLOC', 'System')
                and datediff(minutes, d.lock_datetime, d.quantity_datetime)/60 between 0 and 24
           group by d.plant_id, d.customer_id, d.user_id, d.task_id, d.cube, d.sequence_number, d.inbound_license, d.rrn, 
    d.product_id, d.lock_date, d.lock_datetime  
       ) tdh
             on td.plant_id = tdh.plant_id
             and td.customer_id = tdh.customer_id
              and td.user_id = tdh.user_id
              and td.task_id = tdh.task_id
              and td.lock_date = tdh.lock_date
              and td.month = tdh.month
              and td.year = tdh.year
            group by th.plant_id, th.customer_id,trim(th.user), th.task_id, td.month, td.year
), CASE_PICK_HOUR_CUST AS (
select plant_id, customer_id, user, month, year, sum(case_pick_hours_cust) case_pick_hours_cust from (
select plant_id, customer_id, trim(user) user, month(lock_datetime) month, year(lock_datetime) year,  sum(datediff(minutes, lock_datetime, quantity_datetime))/60 case_pick_hours_cust
from TRANSFORM.FACT_TASK_HISTORY
where year(lock_datetime)=RUN_YEAR and month(lock_datetime)=RUN_MONTH
and qualifier_type in ('BP', 'PL', 'LP', 'BA', 'OP', 'PT', 'RP')
and trim(user) not in ('AUTOLOC', 'System')
and datediff(minutes, lock_datetime, quantity_datetime)/60 between 0 and 24
and customer_id <> 0
and qualifier_type not in ('RO', 'PA', 'RA')
and (plant_id, customer_id, trim(user), task_id, month(lock_datetime), year(lock_datetime)) not in (
select th.plant_id, th.customer_id, th.user, th.task_id, th.month, th.year
  from DOUBLE_STACK th
  left join TRANSFORM.DIM_CUSTOMER_PROFILE c
on th.customer_id = c.customer_id
left join TRANSFORM.FACT_TASK_HISTORY_DETAILS d
on d.plant_id = th.plant_id
and d.customer_id = th.customer_id
and d.task_id = th.task_id
left join TRANSFORM.DIM_PRODUCT_MASTER p
on p.customer_id = th.customer_id
and p.product_id = d.product_id
where th.month = RUN_MONTH
and year(d.lock_datetime) = RUN_YEAR
and trim(c.guideline) = 'RF051D'
and d.qualifier_id = 'BP'
and d.quantity = p.pallet_factor
and trim(d.user_id) not in ('AUTOLOC', 'System')
and datediff(minutes, lock_datetime, quantity_datetime)/60 between 0 and 24
  ) 
  group by plant_id, customer_id, user, month, year
  ) 
 group by plant_id, customer_id, user, month, year
  )
  select * from CASE_PICK_HOUR_CUST where month = RUN_MONTH and year = RUN_YEAR;


MERGE INTO TRANSFORM.PRFT_CUST_CASE_PICK_HOURS TGT
USING (  
WITH TASK_INTERSECTION AS (
select 
plant_id, customer_id, ticket_id, task_id, qualifier_type, trim(user) user,
license, lock_date, lock_time, lock_datetime, quantity_date, quantity_time, quantity_datetime,
sum(datediff(minutes, lock_datetime, quantity_datetime))/60 total_time_user
from TRANSFORM.FACT_TASK_HISTORY
where qualifier_type in ('BP', 'RO', 'PL') 
and month(lock_datetime) = RUN_MONTH
and year(lock_datetime) = RUN_YEAR
and trim(user) not in ('AUTOLOC', 'System')
and datediff(minutes, lock_datetime, quantity_datetime)/60 between 0 and 24
and task_id not in (
select th.task_id 
from TRANSFORM.FACT_TASK_HISTORY th
inner join (select plant_id, customer_id, user_id, task_id, lock_date, month, year 
  from (
  select plant_id, customer_id, user_id, task_id, cube, sequence_number, inbound_license, rrn, product_id, lock_date, 
     month(lock_datetime) month, year(lock_datetime) year--, count(*) task_count
            from TRANSFORM.FACT_TASK_HISTORY_DETAILS
           where month = RUN_MONTH and year = RUN_YEAR and qualifier_id = 'BP' 
           and trim(user_id) not in ('AUTOLOC', 'System')
           and datediff(minutes, lock_datetime, quantity_datetime)/60 between 0 and 24
           group by plant_id, customer_id, user_id, task_id, cube, sequence_number, inbound_license, rrn, product_id, lock_date, lock_datetime--, lock_datetime, quantity_datetime
   ) 
  group by plant_id, customer_id, user_id, task_id, lock_date, month, year
  having count(*) = 2) td
on th.plant_id = td.plant_id
and th.customer_id = td.customer_id
and th.task_id = td.task_id
left join TRANSFORM.DIM_CUSTOMER_PROFILE c
on th.customer_id = c.customer_id
left join TRANSFORM.FACT_TASK_HISTORY_DETAILS d
on d.plant_id = th.plant_id
and d.customer_id = th.customer_id
and d.task_id = th.task_id
left join TRANSFORM.DIM_PRODUCT_MASTER p
on p.customer_id = th.customer_id
and p.product_id = d.product_id
where month(th.lock_datetime) = RUN_MONTH
and month(th.lock_datetime) = RUN_MONTH
and year(d.lock_datetime) = RUN_YEAR
and year(d.lock_datetime) = RUN_YEAR
and trim(c.guideline) = 'RF051D'
and th.qualifier_type = 'BP'
and trim(d.user_id) not in ('AUTOLOC', 'System')
and datediff(minutes, d.lock_datetime, d.quantity_datetime)/60 between 0 and 24
and d.quantity = p.pallet_factor
)
group by plant_id, customer_id, ticket_id, task_id, qualifier_type, user,
license, lock_date, lock_time, lock_datetime, quantity_date, quantity_time, quantity_datetime
), BREAK_TASK_HOURS as (
  select ti.plant_id, ti.customer_id br_customer_id, ti.task_id, ti.qualifier_type, trim(ti.user) user,
ti.lock_date break_startdate, ti.lock_time break_starttime, ti.lock_datetime break_start_datetime, 
ti.quantity_datetime break_end_datetime,
sum(datediff(minutes, ti.lock_datetime, ti.quantity_datetime))/60 br_total_time_user
from TRANSFORM.FACT_TASK_HISTORY  ti
where ti.qualifier_type in ('BR', 'LU')
and month(ti.lock_datetime) = RUN_MONTH
and year(ti.lock_datetime) = RUN_YEAR
and trim(ti.user) not in ('AUTOLOC', 'System')
and datediff(minutes, ti.lock_datetime, ti.quantity_datetime)/60 between 0 and 24
group by  ti.plant_id, ti.customer_id, ti.task_id, ti.qualifier_type, ti.user,
ti.lock_date, ti.lock_time, ti.lock_datetime, 
ti.quantity_date, ti.lock_datetime, ti.quantity_datetime
)  , BREAK_HOUR_INTERSECTION as (
select plant_id, customer_id, user, month, year, sum(br_intersect_time_user) br_intersect_time_cust from (
select bt.plant_id, ti.customer_id, ti.user, ti.task_id, bt.task_id break_task_id,  ti.qualifier_type, bt.qualifier_type break_qualifier_type, 
ti.lock_datetime, ti.quantity_datetime, bt.break_start_datetime, bt.break_end_datetime, month(lock_datetime) month, year(lock_datetime) year,
sum(datediff(minutes, bt.break_start_datetime, bt.break_end_datetime))/60 br_intersect_time_user,
case when bt.break_start_datetime between ti.lock_datetime and ti.quantity_datetime
and bt.break_end_datetime between ti.lock_datetime and ti.quantity_datetime
then 1
else 2
end as break_intersection
from BREAK_TASK_HOURS bt 
left join TASK_INTERSECTION  ti
on ti.plant_id = bt.plant_id 
and trim(ti.user) = trim(bt.user)
where bt.qualifier_type in ('BR', 'LU')
  and month(ti.lock_datetime) = RUN_MONTH
and year(ti.lock_datetime) = RUN_YEAR
and break_intersection = 1
group by  bt.plant_id, ti.customer_id, ti.user, ti.task_id, bt.task_id, ti.qualifier_type, bt.qualifier_type, 
ti.lock_datetime, ti.quantity_datetime, bt.break_start_datetime, bt.break_end_datetime
)
  group by plant_id, customer_id, user, month, year
) 
select bh.plant_id, bh.customer_id, bh.user, cp.month, cp.year, cp.case_pick_hours_cust, cp.case_pick_hours_cust - sum(bh.br_intersect_time_cust) adj_case_pick_hours_cust
from BREAK_HOUR_INTERSECTION bh
left join (select plant_id, customer_id, user, month, year, sum(case_pick_hours_cust) case_pick_hours_cust from TRANSFORM.PRFT_CUST_CASE_PICK_HOURS
          group by plant_id, customer_id, user, month, year) cp
on bh.plant_id = cp.plant_id
and bh.customer_id = cp.customer_id
and bh.user = cp.user
and bh.month = cp.month
and bh.year = cp.year
where cp.month = RUN_MONTH
and cp.year = RUN_YEAR
group by bh.plant_id, bh.customer_id, bh.user, cp.month, cp.year, cp.case_pick_hours_cust
) STG
ON TGT.PLANT_ID = STG.PLANT_ID
AND TGT.CUSTOMER_ID = STG.CUSTOMER_ID
AND TGT.USER = STG.USER
AND TGT.MONTH = STG.MONTH
AND TGT.YEAR = STG.YEAR
WHEN MATCHED THEN
UPDATE
SET TGT.CASE_PICK_HOURS_CUST = STG.ADJ_CASE_PICK_HOURS_CUST;

DELETE FROM TRANSFORM.PRFT_CUST_HANDLING_HOURS 
WHERE YEAR=RUN_YEAR AND MONTH=RUN_MONTH;

insert into TRANSFORM.PRFT_CUST_HANDLING_HOURS
select plant_id, customer_id, user, month, year, sum(handling_hours_cust) handling_hours_cust from (
select eth.plant_id, eth.customer_id, eth.user, 
   eth.month, eth.year,
sum(nvl(eth.total_hours_cust,0) - nvl(ecp.case_pick_hours_cust,0)) handling_hours_cust
from TRANSFORM.PRFT_CUSTOMER_TOTAL_HOURS eth
left join TRANSFORM.PRFT_CUST_CASE_PICK_HOURS ecp
  on eth.plant_id = ecp.plant_id
  and eth.customer_id = ecp.customer_id
and eth.user = ecp.user
and eth.month = ecp.month
and eth.year = ecp.year
group by eth.plant_id, eth.customer_id, eth.user, 
   eth.month, eth.year
  )
where year=RUN_YEAR and month=RUN_MONTH  
  group by plant_id, customer_id, user, month, year;

DELETE FROM TRANSFORM.PRFT_EMP_CUST_WAGE
WHERE YEAR=RUN_YEAR AND MONTH=RUN_MONTH;

insert into TRANSFORM.PRFT_EMP_CUST_WAGE
select hw.plant_id, hw.customer_id, hw.user, hw.month, hw.year, hw.account, hw.subaccount,
 --cp.case_pick_hours_cust, tot.case_pick_hours_tot_user, 
 cw.case_pick_cust_user_wage, hw.handling_cust_user_wage --bw.blast_freeze_cust_user_wage 
 from(
     select h.plant_id, h.customer_id, h.user, h.month, h.year, w.account, w.subaccount, handling_hours_cust, handling_hours_tot_user, 
   iff(tot.handling_hours_tot_user > 0, (h.handling_hours_cust/tot.handling_hours_tot_user) * w.handling_wage_user, 0) handling_cust_user_wage from (
      (select plant_id, customer_id, user, month, year, sum(handling_hours_cust) handling_hours_cust from TRANSFORM.PRFT_CUST_HANDLING_HOURS 
    group by plant_id, customer_id,user, month, year) h
    left join 
    (select plant_id, user, month, year, sum(handling_hours_cust) handling_hours_tot_user from TRANSFORM.PRFT_CUST_HANDLING_HOURS 
    group by plant_id, user,  month, year) tot
    on h.plant_id = tot.plant_id 
    and h.user = tot.user
    and h.month = tot.month
    and h.year = tot.year
    left join 
    (select plant_id, user, month, year, account, subaccount, handling_wage_user from TRANSFORM.PRFT_EMP_HOUR_WAGE ) w
    on h.plant_id = w.plant_id
    and h.user = w.user
    and h.month = w.month
    and h.year = w.year
    )
 ) hw 
 left join  
 (
 select cp.plant_id, cp.customer_id, cp.user, cp.month, cp.year, w.account, w.subaccount, --cp.case_pick_hours_cust, tot.case_pick_hours_tot_user, 
   iff(tot.case_pick_hours_tot_user > 0, (cp.case_pick_hours_cust/tot.case_pick_hours_tot_user) * w.case_pick_wage_user, 0) case_pick_cust_user_wage from (
 --(cp.case_pick_hours_cust/tot.case_pick_hours_tot_user) * w.case_pick_wage_user case_pick_cust_user_wage from (
(select plant_id, customer_id, user, month, year, sum(case_pick_hours_cust) case_pick_hours_cust from TRANSFORM.PRFT_CUST_CASE_PICK_HOURS 
group by plant_id, customer_id,user,  month, year) cp
left join 
(select plant_id, user, month, year, sum(case_pick_hours_cust) case_pick_hours_tot_user from TRANSFORM.PRFT_CUST_CASE_PICK_HOURS 
group by plant_id, user,  month, year) tot
on cp.plant_id = tot.plant_id 
and cp.user = tot.user
and cp.month = tot.month
and cp.year = tot.year
left join 
(select plant_id, user, month, year, account, subaccount, case_pick_wage_user from TRANSFORM.PRFT_EMP_HOUR_WAGE ) w
on cp.plant_id = w.plant_id
and cp.user = w.user
and cp.month = w.month
and cp.year = w.year
)
 )cw
 on cw.plant_id = hw.plant_id
 and cw.customer_id = hw.customer_id
 and cw.user = hw.user
 and cw.month = hw.month
 and cw.year = hw.year
where hw.year=RUN_YEAR and hw.month=RUN_MONTH;
