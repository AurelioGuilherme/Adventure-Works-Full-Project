
with 
    orders as (
        select  
            fk_sales_order
            , pk_sales_order_detail
            , fk_product
            , fk_customer
            , fk_vendor
            , fk_ship_to_address
            , pk_sales_order
            , fk_territory_sales
            , fk_credit_card
            , online_order_flag_description
            , order_date_dt
            , ship_date_dt
            , due_date_dt
            , sales_status
            , status_name
            , unit_price
            , unit_price_discount
            , order_qty
            , sub_total_sale
            , sales_sub_total
            , sales_tax_amt
            , sales_freight
            , total_due
        from {{ ref('int_orders') }}
    )

    , product as (
        select  
             pk_product
            , name_product
            , product_category_name
            , sub_category_name
            , gender_category_description
            , product_line
            , product_line_name
            , product_class
            , product_class_name
            , standard_cost
            , selling_price
            , is_final_product
            , is_manufactured
            , margin_amount
        from {{ ref('dim_products') }}
    )

    , customer as (
        select  
            pk_customer 
            , fk_person 
            , fk_territory_sales
            , email_promotion
            , name_store
            , full_name as customer_full_name
        from {{ ref('dim_customers') }}
    )

    , address_customer as (
        select  
            pk_address
            , full_address
            , city
            , name_province
            , name_state
            , country_region_code
            , territory_name
            , country_code
            , territory_group
        from {{ ref('dim_address') }}
    )


    , dim_dates as (
        select 
            date_day
            , quarter_of_year
            , year_number
            , quarter_start_date
            , quarter_end_date
        from {{ ref('dim_dates') }}
    )

    , join_fact_sales_forecast as (
        select
            -- Chaves surrogadas e ids
            orders.pk_sales_order_detail
            , orders.fk_product
            , orders.pk_sales_order
            , customer.pk_customer
            , product.pk_product
            , address_customer.pk_address
            , orders.fk_sales_order
            , orders.fk_credit_card

            -- Datas e status
            , orders.order_date_dt
            , orders.ship_date_dt
            , orders.due_date_dt
            , orders.online_order_flag_description
            , orders.sales_status
            , orders.status_name
            , dt.date_day
            , dt.quarter_of_year
            , dt.year_number
            , dt.quarter_start_date
            , dt.quarter_end_date

            -- Métricas de venda
            , orders.order_qty
            , orders.unit_price
            , orders.unit_price_discount
            , orders.sub_total_sale
            , orders.sales_sub_total
            , product.standard_cost
            , product.selling_price
            , orders.sales_tax_amt
            , orders.sales_freight
            , orders.total_due

            -- Descritivos
            , customer.customer_full_name
            , customer.name_store
            , product.name_product
            , product.product_category_name
            , product.sub_category_name
            , product.product_line
            , product.product_line_name
            , product.product_class
            , product.product_class_name
            , product.gender_category_description
            , product.is_manufactured
            , product.margin_amount
            , address_customer.full_address
            , address_customer.city
            , address_customer.territory_name
            , address_customer.name_province
            , address_customer.name_state
            , address_customer.country_region_code
            , address_customer.country_code
            , address_customer.territory_group

        from orders
        left join product
            on orders.fk_product = product.pk_product

        left join customer
            on orders.fk_customer = customer.pk_customer

        left join address_customer
            on orders.fk_ship_to_address = address_customer.pk_address

        left join dim_dates as dt
            on orders.due_date_dt = dt.date_day
    )

select *
from join_fact_sales_forecast