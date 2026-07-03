with customers as (
    select * from {{ ref('stg_users') }}
)
select
    user_id       as customer_id,
    full_name,
    email,
    username,
    phone,
    website
from customers
