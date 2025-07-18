with 
    orders_headers as (
        select
            pk_sales_order
            , fk_customer
            , fk_ship_to_address
            , fk_ship_method
            , fk_territory_sales
            , fk_credit_card
            , order_date_dt
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
            , oh.fk_ship_to_address
            , oh.fk_ship_method
            , oh.pk_sales_order
            , oh.fk_territory_sales
            , oh.fk_credit_card
            , oh.order_date_dt
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

    , deduplication as (
        select
            *
            , row_number() over (
                partition by pk_sales_order_detail, pk_sales_order
                order by pk_sales_order
            ) as row_num
        from orders
        qualify row_num = 1
    )

select *
from deduplication
