create streaming table sales_pl 
select *,current_date as ingestion_date from stream read_files('s3://jpmctraining/pipeline_input/sales',format=>'csv');

/*
create streaming table deepak_silver.sales_cleaned_pl
(constraint valid_order_id EXPECT (order_id IS NOT NULL) on violation drop row)
as
select distinct * from stream sales_pl;
*/

create streaming table products_pl 
select *,current_date as ingestion_date from stream read_files('s3://jpmctraining/pipeline_input/products',format=>'csv');


create streaming table customers_pl 
select *,current_date as ingestion_date from stream read_files('s3://jpmctraining/pipeline_input/customers',format=>'csv');