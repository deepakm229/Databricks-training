create streaming table deepak_silver.sales_cleaned_pl
(constraint valid_order_id EXPECT (order_id IS NOT NULL) on violation drop row)
as
select distinct * from stream sales_pl;

create or refresh streaming table deepak_silver.products_cleaned_pl;
create or refresh streaming table deepak_silver.customers_cleaned_pl;

create flow product_flow as auto cdc into
deepak_silver.products_cleaned_pl
from stream(products_pl)
keys (product_id)
Apply as delete when
operation = "DELETE"
sequence by
seqNum
COLUMNS * EXCEPT
(operation,seqNum,_rescued_data,ingestion_date)
stored as 
scd type 1;

create flow customers_flow as auto cdc into
deepak_silver.customers_cleaned_pl
from stream(customers_pl)
keys (customer_id)
Apply as delete when
operation = "DELETE"
sequence by
sequenceNum
COLUMNS * EXCEPT
(operation,sequenceNum,_rescued_data,ingestion_date)
stored as 
scd type 2;