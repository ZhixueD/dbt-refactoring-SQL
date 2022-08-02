with

paid_orders as (

    select * from {{ ref('int_orders') }}
),

complete_paid_orders as (
    
    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
        from paid_orders p
        left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
        group by 1
        order by p.order_id

),

final as (
    select 
        p.*,
        row_number() over (order by p.order_id) as transaction_seq,
        row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,
        case when rank() over (partition by customer_id order by p.order_placed_at, p.order_id) = 1
        then 'new'
        else 'return' end as nvsr,
        x.clv_bad as customer_lifetime_value,
        first_value(p.order_placed_at) over (partition by p.customer_id order by p.order_placed_at) as fdos
        from paid_orders as p
        left join complete_paid_orders as x on x.order_id = p.order_id
        order by order_id
)

select * from final