with customers as (

    select *
    from {{source("child_bronze", "customers")}}

),
deduplicate as (

select *,
row_number() over(partition by customer_id order by read_timestamp) as numeracion
from customers
),

Trimear as (

select
    customer_id,
    trim(customer_name) as customer_name,
    city,
    file_name,
    file_size,
    read_timestamp
from deduplicate
where numeracion=1 
),

cleaned as (

    select
        * except(city),

        case
            when city in ('Bengaluruu', 'Bengalore')
                then 'Bengaluru'

            when city in ('Hyderabadd', 'Hyderbad')
                then 'Hyderabad'

            when city in ('NewDelhi', 'NewDheli', 'NewDelhee')
                then 'New Delhi'

            else city
        end as city

    from Trimear
    where city is not null
)

select 
    customer_id,
    initcap(customer_name) as customer_name,
    file_name,
    file_size,
    read_timestamp,
    city
from cleaned
where customer_name is not null

