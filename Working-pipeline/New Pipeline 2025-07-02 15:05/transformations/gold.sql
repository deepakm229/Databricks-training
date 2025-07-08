create materialized view if not exists deepak_gold.customer_active as 
select customer_name,customer_id,customer_email,customer_city,customer_state from deepak_silver.customers_cleaned_pl where `__END_AT` is null;

create materialized view if not exists deepak_gold.top_3_sales as
select
sc.customer_id,
a.customer_name,
a.customer_email,
sum(sc.total_amount) as total_sales
from deepak_silver.sales_cleaned_pl sc
join deepak_gold.customer_active a on sc.customer_id = a.customer_id
group by all
order by total_sales desc
limit 3