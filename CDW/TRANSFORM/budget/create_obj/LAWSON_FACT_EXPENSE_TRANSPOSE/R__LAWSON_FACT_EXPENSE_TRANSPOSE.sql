USE SCHEMA TRANSFORM;

create or replace view LAWSON_FACT_EXPENSE_TRANSPOSE as
select d.plant_id, d.department, d.gl_company, d.account_unit, d.account, d.sub_account, d.fiscal_year, d.month, concat(lpad(month(to_date(d.month,'Mon')),2,0), '/', '01', '/', d.fiscal_year) mmddyyyy, d.expense_debit, c.expense_credit from 
(select plant_id, department, gl_company, account_unit, account, sub_account, fiscal_year, substr(debit_month,14,3) as month, expense_debit from (
select plant_id, department, gl_company, account_unit, account, sub_account, fiscal_year, debit_month, expense_debit from lawson_fact_expense
    unpivot(expense_debit for debit_month in (debit_amount_jan, debit_amount_feb, debit_amount_mar, debit_amount_apr, debit_amount_may, debit_amount_jun,
                               debit_amount_jul, debit_amount_aug, debit_amount_sep, debit_amount_oct, debit_amount_nov, debit_amount_dec))
  )
 ) d
 join
 (select plant_id, department, gl_company, account_unit, account, sub_account, fiscal_year, substr(credit_month,15,3) as month, expense_credit from (
select plant_id, department, gl_company, account_unit, account, sub_account, fiscal_year, credit_month, expense_credit from lawson_fact_expense
    unpivot(expense_credit for credit_month in (credit_amount_jan, credit_amount_feb, credit_amount_mar, credit_amount_apr, credit_amount_may, credit_amount_jun,
                               credit_amount_jul, credit_amount_aug, credit_amount_sep, credit_amount_oct, credit_amount_nov, credit_amount_dec))
  )
 ) c   
  on d.plant_id = c.plant_id 
    and d.department = c.department
    and d.gl_company = c.gl_company
    and d.account_unit = c.account_unit
    and d.account = c.account
    and d.sub_account = c.sub_account
    and d.fiscal_year = c.fiscal_year
    and d.month = c.month
;
