with source as (
    select * from {{ source('raw', 'todos') }}
)
select
    cast(id as integer)        as order_id,
    cast(userid as integer)    as customer_id,
    trim(title)                as order_description,
    completed                  as is_completed
from source
where id is not null
