with 
    orders as (
        select  
            fk_sales_order
            , pk_sales_order_detail
            , fk_sales_order
            , fk_product
            , fk_customer
            , fk_vendor
            , fk_ship_to_address
            , fk_ship_method
            , pk_sales_order
            , fk_territory_sales
            , fk_credit_card
            , online_order_flag
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
            , gender_category_product
            , product_line
            , product_class
            , standard_cost
            , selling_price
            , is_final_product
            , is_manufactured
        from {{ ref('dim_products') }}
    )

    , customer as (
        select  
            pk_customer 
            , fk_person 
            , fk_territory_sales
            , email_promotion
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

    , vendor as (
        select  
            pk_vendor
            , full_name as vendor_full_name
            , commission_sales_pct
        from {{ ref('dim_vendor') }}
    )

    , ship_method as (
        select  
            pk_ship_method
            , shipping_name
        from {{ ref('dim_ship_method') }}
    )

    , credit_card as (
        select  
            pk_credit_card
            , card_type
        from {{ ref('stg_mssql__sales_credit_card') }}
    )

    , reason_header as (
        select
            fk_sales_order
            , pk_sales_reason
        from {{ ref('stg_mssql__sales_reason_header') }}

    )

    , reason as (
        select  
            fk_sales_reason
            , reason_name
            , reason_type
        from {{ ref('dim_reason') }}
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

    , join_fact_sales as (
        select
            -- Chaves surrogadas e ids
            orders.pk_sales_order_detail
            , orders.fk_product
            , orders.pk_sales_order
            , customer.pk_customer
            , product.pk_product
            , vendor.pk_vendor
            , address_customer.pk_address
            , ship_method.pk_ship_method
            , reason.fk_sales_reason
            , orders.fk_sales_order
            , orders.fk_credit_card

            -- Datas e status
            , orders.order_date_dt
            , orders.ship_date_dt
            , orders.due_date_dt
            , orders.online_order_flag
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
            , vendor.commission_sales_pct

            -- Descritivos
            , customer.customer_full_name
            , product.name_product
            , product.product_category_name
            , product.sub_category_name
            , product.product_line
            , product.product_class
            , product.gender_category_product
            , product.is_manufactured
            , vendor.vendor_full_name
            , ship_method.shipping_name
            , address_customer.full_address
            , address_customer.city
            , address_customer.territory_name
            , address_customer.name_province
            , address_customer.name_state
            , address_customer.country_region_code
            , address_customer.country_code
            , address_customer.territory_group
            , credit_card.card_type
            , reason.reason_name
            , reason.reason_type

        from orders
        left join product
            on orders.fk_product = product.pk_product

        left join customer
            on orders.fk_customer = customer.pk_customer

        left join address_customer
            on orders.fk_ship_to_address = address_customer.pk_address

        left join vendor
            on orders.fk_vendor = vendor.pk_vendor

        left join ship_method
            on orders.fk_ship_method = ship_method.pk_ship_method

        left join credit_card 
            on orders.fk_credit_card = credit_card.pk_credit_card

        left join reason_header
            on orders.fk_sales_order = reason_header.fk_sales_order

        left join reason
            on reason_header.pk_sales_reason = reason.fk_sales_reason

        left join dim_dates as dt
            on orders.due_date_dt = dt.date_day
    )

select *
from join_fact_sales
