with source as (
    select * from {{ source('raw', 'users') }}
),
staged as (
    select
        cast(id as integer)    as user_id,
        trim(name)             as full_name,
        lower(trim(email))     as email,
        lower(trim(username))  as username,
        trim(phone)            as phone,
        trim(website)          as website
    from source
    where id is not null
)
select * from staged
