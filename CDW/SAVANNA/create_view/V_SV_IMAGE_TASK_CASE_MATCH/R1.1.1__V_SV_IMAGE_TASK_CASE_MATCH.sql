USE SCHEMA SAVANNA;

create view V_SV_IMAGE_TASK_CASE_MATCH as
WITH CASE_MATCH AS
(
select image_id, monthname(created_date) month, year(created_date) year, created_date, 
case when analysis_pallet_type <> 'WHITE'
then count(analysis_pallet_type)/(select count(analysis_pallet_type) from SV_FACT_IMAGE_ANALYSIS_TASK where analysis_pallet_type <> 'WHITE' and created_date between '2022-08-28' and current_date ) * 100 
else 0
end as case_mismatch_pct,
case when analysis_pallet_type = 'WHITE'
then count(analysis_pallet_type)/(select count(analysis_pallet_type) from SV_FACT_IMAGE_ANALYSIS_TASK where analysis_pallet_type = 'WHITE' and created_date between '2022-08-28' and current_date ) * 100 
else 0
end as case_match_pct
from SV_FACT_IMAGE_ANALYSIS_TASK 
Where created_date between '2022-08-28' and current_date
group by image_id, month, year, created_date, analysis_pallet_type
),
CASE_COUNT_MATCH AS 
(
select image_id, monthname(created_date) month, year(created_date) year, created_date, 
case when case_count_accepted = 0
then count(case_count_accepted)/(select count(case_count_accepted) from SV_FACT_IMAGE_ANALYSIS_TASK where case_count_accepted = 0 and created_date between '2022-08-28' and current_date ) * 100 
else 0
end as case_count_mismatch_pct,
case when case_count_accepted = 1
then count(case_count_accepted)/(select count(case_count_accepted) from SV_FACT_IMAGE_ANALYSIS_TASK where case_count_accepted = 1 and created_date between '2022-08-28' and current_date ) * 100 
else 0
end as case_count_match_pct
from SV_FACT_IMAGE_ANALYSIS_TASK 
Where created_date between '2022-08-28' and current_date
group by image_id, month, year, created_date, case_count_accepted
),
IMAGE_ANALYSIS AS
(
select plant_id, image_id, created_date, monthname(created_date) month, year(created_date) year, container_id, lpn, notes, process_message, reviewed, status, case_count, analysis_case_count,
case_count_accepted, pallet_type, analysis_pallet_type, pallet_type_accepted, layers, layers_confidence, cases_per_layer, cases_per_layer_confidence,
pallet_type_confidence, top_type, response_date, customer, warehouse, source_system, 
source_dbms, dl_ins_dt, dl_ins_usr, cdw_ins_dt, cdw_ins_usr, cdw_upd_dt, cdw_upd_usr from SV_FACT_IMAGE_ANALYSIS_TASK
where created_date between '2022-08-28' and current_date   
)
select it.plant_id, it.image_id, it.created_date, it.month, it.year, 
cs.case_match_pct, cs.case_mismatch_pct, ccm.case_count_match_pct, ccm.case_count_mismatch_pct,
it.container_id, it.lpn, it.notes, it.process_message, it.reviewed, it.status, it.case_count, it.analysis_case_count,
it.case_count_accepted, it.pallet_type, it.analysis_pallet_type, it.pallet_type_accepted, it.layers, it.layers_confidence, it.cases_per_layer, it.cases_per_layer_confidence,
pallet_type_confidence, top_type, response_date, customer, warehouse, source_system, 
it.source_dbms, it.dl_ins_dt, it.dl_ins_usr, it.cdw_ins_dt, it.cdw_ins_usr, it.cdw_upd_dt, it.cdw_upd_usr
from IMAGE_ANALYSIS it
left join CASE_MATCH cs
on it.image_id = cs.image_id
and it.month = cs.month
and it.year = cs.year
and it.created_date = cs.created_date
left join CASE_COUNT_MATCH ccm
on it.image_id = ccm.image_id
and it.month = ccm.month
and it.year = ccm.year
and it.created_date = ccm.created_date
where it.created_date between '2022-08-28' and current_date 
order by created_date 
;
