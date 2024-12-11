with calendar_months as (
   select
       calendar_month
   from
       unnest(generate_date_array(date_sub(date_trunc(current_date, month), interval 36 month), date_trunc(current_date, month), interval 1 month)) as calendar_month   
)

select * from calendar_months