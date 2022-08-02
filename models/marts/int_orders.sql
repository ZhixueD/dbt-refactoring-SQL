with

customers as (
    select * from {{ ref('stg_jaffle_shop_customers') }}
),

orders as (
    select * from {{ ref('stg_jaffle_shop_orders') }}
),

payment as (
    select * from {{ ref('stg_stripe_payments') }}
),

complete_payment as (

    select order_id, max(payment_created_at) as payment_finalized_date, 
    sum(payment_amount) as total_amount_paid
    from payment
    where payment_status <> 'fail'
    group by 1
),

paid_orders as (
    select orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        complete_payment.total_amount_paid,
        complete_payment.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name,
        customers.full_name
    from orders
    left join complete_payment
        ON orders.order_id = complete_payment.order_id
        left join customers on orders.customer_id = customers.customer_id )

select * from paid_orders