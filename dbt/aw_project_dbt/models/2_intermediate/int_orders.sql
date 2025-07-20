with 
    orders_headers as (
        select
            pk_sales_order
            , fk_customer
            , fk_ship_to_address
            , fk_ship_method
            , fk_territory_sales
            , fk_credit_card
            , fk_vendor
            , online_order_flag
            , sales_status
            , order_date_dt
            , due_date_dt
            , ship_date_dt
            , sales_sub_total
            , sales_tax_amt
            , sales_freight
            , total_due
        from {{ ref("stg_api__salesorderheader") }}
    )

    , orders_details as (
        select
            pk_sales_order_detail
            , fk_sales_order
            , fk_product
            , unit_price
            , unit_price_discount
            , order_qty
            , sub_total_sale
        from {{ ref("stg_mssql__sales_order_detail") }}
    )

    , orders as (
        select
            od.pk_sales_order_detail
            , od.fk_sales_order
            , od.fk_product
            , oh.fk_customer
            , oh.fk_vendor
            , oh.fk_ship_to_address
            , oh.fk_ship_method
            , oh.pk_sales_order
            , oh.fk_territory_sales
            , oh.fk_credit_card
            , oh.online_order_flag
            , oh.order_date_dt
            , oh.ship_date_dt
            , oh.due_date_dt
            , oh.sales_status
            , od.unit_price
            , od.unit_price_discount
            , od.order_qty
            , od.sub_total_sale
            , oh.sales_sub_total
            , oh.sales_tax_amt
            , oh.sales_freight
            , oh.total_due
        from orders_headers  as oh
        left join orders_details as od
            on oh.pk_sales_order = od.fk_sales_order
    )

select *
from orders
