/* 
Dataset: The look of Ecommerce
Platform: Bigquery
Skills used: Aggregate Functions, CTE's, Temp Tables, Joins, Creating Views, Data Types, ...
*/

-- 1. Inspecting the dataset

-- gather information about the dataset
select table_name, table_type, ddl
from bigquery-public-data.thelook_ecommerce.INFORMATION_SCHEMA.TABLES
;

-- view schema of tables
select *
from bigquery-public-data.thelook_ecommerce.INFORMATION_SCHEMA.COLUMNS
order by table_name;

-- previewing the initial data
select *
from `bigquery-public-data.thelook_ecommerce.order_items` limit 1000;
select *
from `bigquery-public-data.thelook_ecommerce.products`limit 1000;

-- 2. Ecommerce Metrics & KPIs

/* Explore key metrics for evaluating Ecommerce Revenue
GMV: Sum of sales volume transacting through the platform
NMV: Sum of sales volume of successful delivery through the platform
Gross margin = NMV - COGS 
COGS per order
revenue per order
*/
select distinct
format_timestamp('%Y-%m',ifnull(a.delivered_at,created_at),'Asia/Ho_Chi_Minh') month,
sum(a.sale_price) as GMV,
sum(case when status = 'Complete' then a.sale_price else 0 end) as net_revenue,
sum(case when status = 'Complete'  then a.sale_price-b.cost else 0 end) as gross_margin,
sum(case when status = 'Complete'  then a.sale_price-b.cost else 0 end)/sum(case when status = 'Complete' then a.sale_price else 0 end) as percent_gross_margin,
sum(case when status = 'Complete' then b.cost else 0 end)/count(distinct case when status = 'Complete' then a.order_id end) as COGS_per_order,
sum(case when status = 'Complete' then a.sale_price else 0 end)/count(distinct case when status = 'Complete' then a.order_id end) as revenue_per_order
from `bigquery-public-data.thelook_ecommerce.order_items` a
left join `bigquery-public-data.thelook_ecommerce.products` b on a.product_id = b.id
group by 1
order by 1;

-- Compare Profit by Product Categories
-- Display the percentages of sales by Product Categories in the past 3 years
select distinct
b.category,
b.brand,
sum(a.sale_price) over (partition by b.category,b.brand ) as total_sales_by_brands,
sum(a.sale_price) over (partition by b.category) as total_sales_by_categories,
sum(a.sale_price) over (partition by b.category,b.brand )/sum(a.sale_price) over (partition by b.category) as percent_sale,
from `bigquery-public-data.thelook_ecommerce.order_items` a
left join `bigquery-public-data.thelook_ecommerce.products` b on a.product_id = b.id
where a.status = 'Complete'
and date(a.created_at,'Asia/Ho_Chi_Minh') >= date_sub(current_date('Asia/Ho_Chi_Minh'), INTERVAL 3 year)
order by 1,5 desc;

-- Using CTE to Perform Calculation on Partition By in Previous Query
-- Identify the top 3 of brands of each category with the highest sales in the past 3 years
with source_data as (
  select distinct
  b.category,
  b.brand,
  sum(sale_price) total_sales_by_brands,
  rank() over (partition by b.category order by sum(sale_price) desc) as rank_num
  from `bigquery-public-data.thelook_ecommerce.order_items` a
  left join `bigquery-public-data.thelook_ecommerce.products` b on a.product_id = b.id
  where a.status = 'Complete'
  and date(a.created_at,'Asia/Ho_Chi_Minh') >= date_sub(current_date('Asia/Ho_Chi_Minh'), INTERVAL 3 year)
  group by 1,2
  order by 1,3 desc
)
select
*
from source_data
where rank_num <= 3;


-- Creating View to store data for later calculation
Create View case_study_1.view1 as 
( select distinct
  b.category,
  b.brand,
  sum(sale_price) total_sales_by_brands,
  rank() over (partition by b.category order by sum(sale_price) desc) as rank_num
  from `bigquery-public-data.thelook_ecommerce.order_items` a
  left join `bigquery-public-data.thelook_ecommerce.products` b on a.product_id = b.id
  where a.status = 'Complete'
  and date(a.created_at,'Asia/Ho_Chi_Minh') >= date_sub(current_date('Asia/Ho_Chi_Minh'), INTERVAL 3 year)
  group by 1,2
  order by 1,3 desc
);


-- Using Temp Table to perform Calculation on Partition By in previous query

create or replace table case_study_1.table1 (
  category string,
  brand string,
  total_sales_by_brands float64,
  rank_num integer
);

insert into case_study_1.table1
(select distinct
  b.category,
  b.brand,
  sum(sale_price) total_sales_by_brands,
  rank() over (partition by b.category order by sum(sale_price) desc) as rank_num
  from `bigquery-public-data.thelook_ecommerce.order_items` a
  left join `bigquery-public-data.thelook_ecommerce.products` b on a.product_id = b.id
  where a.status = 'Complete'
  and date(a.created_at,'Asia/Ho_Chi_Minh') >= date_sub(current_date('Asia/Ho_Chi_Minh'), INTERVAL 3 year)
  group by 1,2
  order by 1,3 desc);

select *
from `case_study_1.table1`







