CALL SCHEMA TRANSFORM;

delete from TRANSFORM.PRFT_EMPLOYEE_TOTAL_HOURS
where year=RUN_YEAR and month=RUN_MONTH;

INSERT INTO TRANSFORM.PRFT_EMPLOYEE_TOTAL_HOURS
select plant_id, trim(user) user, month(lock_datetime) month, year(lock_datetime) year, 
 sum(datediff(minutes, lock_datetime, quantity_datetime))/60 total_hours_emp
from TRANSFORM.FACT_TASK_HISTORY 
where year(lock_datetime)=RUN_YEAR and month(lock_datetime)=RUN_MONTH
and customer_id <> 0
and qualifier_type not in ('RO', 'PA', 'RA')
and trim(user) not in ('AUTOLOC', 'System')
and datediff(minutes, lock_datetime, quantity_datetime)/60 between 0 and 24
group by plant_id, user, month, year;

MERGE INTO TRANSFORM.PRFT_EMPLOYEE_TOTAL_HOURS TGT
USING 
(
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
and trim(user) not in ('AUTOLOC', 'System')
and datediff(minutes, ti.lock_datetime, ti.quantity_datetime)/60 between 0 and 24
group by  ti.plant_id, ti.customer_id, ti.task_id, ti.qualifier_type, ti.user,
ti.lock_date, ti.lock_time, ti.lock_datetime, 
ti.quantity_date, ti.lock_datetime, ti.quantity_datetime
), BREAK_HOUR_INTERSECTION as (
select bt.plant_id, ti.customer_id, ti.task_id, bt.task_id break_task_id,  ti.qualifier_type, bt.qualifier_type break_qualifier_type, bt.user,
ti.lock_datetime, ti.quantity_datetime, bt.break_start_datetime, bt.break_end_datetime, 
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
group by  bt.plant_id, ti.customer_id, ti.task_id, bt.task_id, ti.qualifier_type, bt.qualifier_type, bt.user,
ti.lock_datetime, ti.quantity_datetime, bt.break_start_datetime, bt.break_end_datetime
)
select bh.plant_id, bh.user, ep.month, ep.year, ep.total_hours_emp, ep.total_hours_emp - sum(bh.br_intersect_time_user) adj_total_hours_emp
from BREAK_HOUR_INTERSECTION bh
left join TRANSFORM.PRFT_EMPLOYEE_TOTAL_HOURS ep
on bh.plant_id = ep.plant_id
and month(bh.break_start_datetime) = ep.month
and year(bh.break_start_datetime) = ep.year
and bh.user = ep.user
where ep.month = RUN_MONTH
and ep.year = RUN_YEAR
group by bh.plant_id, bh.user, ep.month, ep.year, ep.total_hours_emp
) STG
ON TGT.PLANT_ID = STG.PLANT_ID
AND TGT.USER = STG.USER
AND TGT.MONTH = STG.MONTH
AND TGT.YEAR = STG.YEAR
WHEN MATCHED THEN
UPDATE
SET TGT.TOTAL_HOURS_EMP = STG.ADJ_TOTAL_HOURS_EMP
;

delete from TRANSFORM.PRFT_USER_CASE_PICK_HOURS
where year=RUN_YEAR and month=RUN_MONTH;

insert into TRANSFORM.PRFT_USER_CASE_PICK_HOURS
select plant_id, user, month, year, sum(case_pick_hours_user) case_pick_hours_user from (
select plant_id, trim(user) user, month(lock_datetime) month, year(lock_datetime) year,  sum(datediff(minutes, lock_datetime, quantity_datetime))/60 case_pick_hours_user
from TRANSFORM.FACT_TASK_HISTORY
where year(lock_datetime)=RUN_YEAR and month(lock_datetime)=RUN_MONTH
and qualifier_type in ('BP', 'PL', 'LP', 'BA', 'OP', 'PT', 'RP')
and customer_id <> 0
and qualifier_type not in ('RO', 'PA', 'RA')
and trim(user) not in ('AUTOLOC', 'System')
and datediff(minutes, lock_datetime, quantity_datetime)/60 between 0 and 24
and task_id not in (
select th.task_id 
from TRANSFORM.FACT_TASK_HISTORY th
inner join (select plant_id, customer_id, user_id, task_id, lock_date, month, year 
  from (
  select plant_id, customer_id, user_id, task_id, cube, sequence_number, inbound_license, rrn, product_id, lock_date, 
     month(lock_datetime) month, year(lock_datetime) year
            from TRANSFORM.FACT_TASK_HISTORY_DETAILS
           where month = RUN_MONTH and year = RUN_YEAR and qualifier_id = 'BP' and trim(user_id) not in ('AUTOLOC', 'System') and datediff(minutes, lock_datetime, quantity_datetime)/60 between 0 and 24
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
  group by  plant_id, customer_id, trim(user), month(lock_datetime), year(lock_datetime)
  )
  group by plant_id, user, month, year;

MERGE INTO TRANSFORM.PRFT_USER_CASE_PICK_HOURS TGT
USING 
( 
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
     month(lock_datetime) month, year(lock_datetime) year
            from TRANSFORM.FACT_TASK_HISTORY_DETAILS
           where month = RUN_MONTH and year = RUN_YEAR and qualifier_id = 'BP' and trim(user_id) not in ('AUTOLOC', 'System') and datediff(minutes, lock_datetime, quantity_datetime)/60 between 0 and 24
           group by plant_id, customer_id, user_id, task_id, cube, sequence_number, inbound_license, rrn, product_id, lock_date, lock_datetime
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
), BREAK_HOUR_INTERSECTION as (
select bt.plant_id, ti.customer_id, ti.task_id, bt.task_id break_task_id,  ti.qualifier_type, bt.qualifier_type break_qualifier_type, bt.user,
ti.lock_datetime, ti.quantity_datetime, bt.break_start_datetime, bt.break_end_datetime, 
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
group by  bt.plant_id, ti.customer_id, ti.task_id, bt.task_id, ti.qualifier_type, bt.qualifier_type, bt.user,
ti.lock_datetime, ti.quantity_datetime, bt.break_start_datetime, bt.break_end_datetime
)
select bh.plant_id, bh.user, cp.month, cp.year, cp.case_pick_hours_user, cp.case_pick_hours_user - sum(bh.br_intersect_time_user) adj_case_pick_hours_user 
from BREAK_HOUR_INTERSECTION bh
left join TRANSFORM.PRFT_USER_CASE_PICK_HOURS cp
on bh.plant_id = cp.plant_id
and month(bh.break_start_datetime) = cp.month
and year(bh.break_start_datetime) = cp.year
and bh.user = cp.user
where cp.month = RUN_MONTH
and cp.year = RUN_YEAR
group by bh.plant_id, bh.user, cp.month, cp.year, cp.case_pick_hours_user
) STG
ON TGT.PLANT_ID = STG.PLANT_ID
AND TGT.USER = STG.USER
AND TGT.MONTH = STG.MONTH
AND TGT.YEAR = STG.YEAR
WHEN MATCHED THEN
UPDATE
SET TGT.CASE_PICK_HOURS_USER = STG.ADJ_CASE_PICK_HOURS_USER;

delete from TRANSFORM.PRFT_USER_BLAST_FREEZE_HOURS
where year=RUN_YEAR and month=RUN_MONTH;

insert into TRANSFORM.PRFT_USER_BLAST_FREEZE_HOURS
select bf.plant_id, bf.user, bf.month, bf.year, sum(blast_freeze_hours_user) blast_freeze_hours_user from (
select th.plant_id, trim(th.user) user, month(th.lock_datetime) month, year(th.lock_datetime) year, sum(datediff(minutes, th.lock_datetime, th.quantity_datetime))/60 blast_freeze_hours_user 
  from TRANSFORM.FACT_TASK_HISTORY th
  left join TRANSFORM.DIM_LOCATION_MASTER m
  on th.plant_id = m.plant_id
  and (th.to_location = m.location
  or th.from_location = m.location)
  where m.qualifier = 'BL'
  and year(th.lock_datetime)=RUN_YEAR and month(th.lock_datetime)=RUN_MONTH
  and customer_id <> 0
  and trim(th.user) not in ('AUTOLOC', 'System')
  and datediff(minutes, lock_datetime, quantity_datetime)/60 between 0 and 24
  group by th.plant_id, user, month, year
  union
   select plant_id, trim(user) user, month(lock_datetime) month, year(lock_datetime) year, sum(datediff(minutes, lock_datetime, quantity_datetime))/60 blast_freeze_hours_emp 
  from TRANSFORM.FACT_TASK_HISTORY 
  where qualifier_type = 'SP'
  and  year(lock_datetime)=RUN_YEAR and month(lock_datetime)=RUN_MONTH
  and customer_id <> 0
  and trim(user) not in ('AUTOLOC', 'System')
  and datediff(minutes, lock_datetime, quantity_datetime)/60 between 0 and 24
  group by plant_id, user, month, year
  )bf
  group by bf.plant_id, bf.user, bf.month, bf.year;

delete from TRANSFORM.PRFT_USER_HANDLING_HOURS
where year=RUN_YEAR and month=RUN_MONTH;

insert into TRANSFORM.PRFT_USER_HANDLING_HOURS
select plant_id, user, month, year, sum(handling_hours_user) handling_hours_user from (
select eth.plant_id, eth.user, 
   eth.month, eth.year,
 sum((nvl(eth.total_hours_emp,0) - nvl(ecp.case_pick_hours_user,0) - nvl(ebf.blast_freeze_hours_user,0))) handling_hours_user
 from TRANSFORM.PRFT_EMPLOYEE_TOTAL_HOURS eth
 left join TRANSFORM.PRFT_USER_BLAST_FREEZE_HOURS ebf
 on eth.plant_id = ebf.plant_id
 and eth.user = ebf.user
 and eth.month = ebf.month
 and eth.year = ebf.year
 left join TRANSFORM.PRFT_USER_CASE_PICK_HOURS ecp
  on eth.plant_id = ecp.plant_id
 and eth.user = ecp.user
 and eth.month = ecp.month
 and eth.year = ecp.year
group by eth.plant_id,  eth.user,
   eth.month, eth.year
  )
  where year=RUN_YEAR and month=RUN_MONTH
  group by plant_id,user, month, year;
 
DELETE FROM TRANSFORM.PRFT_USER_HOUR_PCT_TOTALS
WHERE YEAR=RUN_YEAR AND MONTH=RUN_MONTH;
 
insert into TRANSFORM.PRFT_USER_HOUR_PCT_TOTALS
select tot.plant_id, pct.user, tot.month, tot.year, pct.case_pick_hours_user, pct.blast_freeze_hours_user, pct.handling_hours_user,
tot.total_hours_emp,
iff(tot.total_hours_emp > 0, (pct.case_pick_hours_user/tot.total_hours_emp) * 100, 0) case_pick_user_hours_pct,
iff(tot.total_hours_emp > 0, (pct.blast_freeze_hours_user/tot.total_hours_emp) * 100, 0)  blast_freeze_user_hours_pct,
iff(tot.total_hours_emp > 0, (pct.handling_hours_user/tot.total_hours_emp) * 100, 0) handling_user_hours_pct
from (
 select plant_id, user, month, year, sum(total_hours_emp) total_hours_emp from TRANSFORM.PRFT_EMPLOYEE_TOTAL_HOURS 
    group by plant_id, user, month, year
  ) tot
left join
(
  select plant_id, user, month, year, 
 sum(nvl(case_pick_hours_user,0)) case_pick_hours_user, 
  sum(nvl(blast_freeze_hours_user,0)) blast_freeze_hours_user, 
  sum(nvl(handling_hours_user,0)) handling_hours_user 
  from (
select plant_id, user, month, year, case_pick_hours_user, null as blast_freeze_hours_user, null as handling_hours_user
  from TRANSFORM.PRFT_USER_CASE_PICK_HOURS
union
  select plant_id, user, month, year, null as case_pick_hours_user, blast_freeze_hours_user, null as handling_hours_user
  from TRANSFORM.PRFT_USER_BLAST_FREEZE_HOURS
union
 select plant_id, user, month, year, null as case_pick_hours_user, null as blast_freeze_hours_user, handling_hours_user
 from TRANSFORM.PRFT_USER_HANDLING_HOURS
  ) 
  group by plant_id, user, month, year
  ) pct 
  on tot.plant_id = pct.plant_id
and tot.user = pct.user
and tot.month = pct.month
and tot.year = pct.year
where tot.year=RUN_YEAR and tot.month=RUN_MONTH
;


DELETE FROM TRANSFORM.PRFT_EMP_HOUR_WAGE
WHERE YEAR=RUN_YEAR AND MONTH=RUN_MONTH;

insert into TRANSFORM.PRFT_EMP_HOUR_WAGE
select pct.plant_id, pct.user, pct.month, pct.year, w.account, w.subaccount,
nvl(pct.case_pick_hours_user,0) case_pick_hours_user, 
nvl(pct.blast_freeze_hours_user,0) blast_freeze_hours_user,
nvl(pct.handling_hours_user,0) handling_hours_user,
nvl(pct.total_hours_emp,0) total_hours_emp,
(pct.case_pick_user_hours_pct * employee_monthly_wage_amount/100) case_pick_wage_user,
(pct.blast_freeze_user_hours_pct * employee_monthly_wage_amount/100) blast_freeze_wage_user,
(pct.handling_user_hours_pct * employee_monthly_wage_amount/100) handling_wage_user,
nvl(w.employee_monthly_wage_amount,0) employee_monthly_wage_amount
from TRANSFORM.PRFT_USER_HOUR_PCT_TOTALS pct
left join (
select plant_id, employee_id, user, user_id, month, year, dst_account account, dst_sub_acct subaccount, sum(employee_monthly_wage_amount) employee_monthly_wage_amount from (
select us.plant_id, w.employee_id, trim(us.user) user, us.user_id, month(w.person_end_date) month, year(w.person_end_date) year, dst_account, dst_sub_acct, wage_amount employee_monthly_wage_amount
from TRANSFORM.HR_DIM_WAGES w 
left join TRANSFORM.DIM_RF_USER us
on us.employee_number = w.employee_id
and us.plant_id = w.process_level
where trim(us.user) not in ('AUTOLOC', 'System')
  )
  group by plant_id, employee_id, user, user_id, month, year, account, subaccount
) w
on pct.plant_id = w.plant_id
and pct.user = w.user
and pct.month = w.month
and pct.year = w.year
where pct.year=RUN_YEAR and pct.month=RUN_MONTH
;
