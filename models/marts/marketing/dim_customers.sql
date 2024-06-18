

with customers as (

   select * from {{ ref('stg_customers') }}
),

orders as (

    select * from {{ ref('stg_orders') }}

),
payments as (
    select * from {{ ref('fct_orders')}}
), 

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

customer_lifetime_amounts as (
     select
        customers.customer_id, 
        SUM(amount) as lifetime_value
    from  customers
    inner join  orders 
    ON customers.customer_id = orders.customer_id
    inner join  payments
    ON orders.order_id = payments.order_id
    -- where payments.status = "success"
    group by customer_id
    

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_lifetime_amounts.lifetime_value, 0) as lifetime_value

    from customers

    left join customer_orders using (customer_id)
    left join customer_lifetime_amounts using (customer_id)

)

select * from final