with customers as (

   select * from {{ ref('stg_customers') }}
),
orders as (

    select * from {{ ref('stg_orders') }}

),
payments as (
    select * from {{ ref('stg_stripe_payments')}}
), 
final as (
     select
        customers.customer_id, 
        orders.order_id, 
        payments.amount
    from  customers
    inner join  orders 
        ON customers.customer_id = orders.customer_id
    inner join  payments
        ON orders.order_id = payments.order_id
    WHERE payments.status = "success"
        
)

select * from final

