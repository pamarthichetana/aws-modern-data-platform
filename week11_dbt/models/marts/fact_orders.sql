with orders as (
    select * from {{ ref('stg_todos') }}
),
customers as (
    select customer_id from {{ ref('dim_customers') }}
)
select
    o.order_id,
    o.customer_id,
    o.order_description,
    o.is_completed
from orders o
inner join customers c on o.customer_id = c.customer_id
