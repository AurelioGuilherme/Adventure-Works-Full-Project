with
    product_details as (
        select 
            pk_product
            , name_product
            , is_manufactured
            , is_final_product
            , product_color
            , safety_stock_level
            , minimal_stock_level
            , standard_cost
            , selling_price
            , days_to_manufacture
            , product_line
            , product_class
            , gender_category_product
            , fk_product_sub_category
            , fk_product_model
            , sell_start_date_dt
            , sell_end_date_dt
            , discontinued_date_dt
        from {{ ref("stg_mssql__product") }}
    )

    , product_category as (
        select
            pk_product_category
            , name_product_category
        from {{ ref("stg_mssql__product_category") }}
    )

    , product_sub_category as (
        select
            pk_product_sub_category
            , fk_product_category
            , sub_category_name
        from {{ ref("stg_mssql__product_sub_category") }}
    )

    , products as (
        select
            pd.pk_product
            , pd.name_product
            , pd.is_manufactured
            , pd.is_final_product
            , pd.product_color
            , pd.safety_stock_level
            , pd.minimal_stock_level
            , pd.standard_cost
            , pd.selling_price
            , pd.days_to_manufacture
            , pd.product_line
            , pd.product_class
            , pd.gender_category_product
            , pd.fk_product_sub_category
            , pd.fk_product_model
            , pd.sell_start_date_dt
            , pd.sell_end_date_dt
            , pd.discontinued_date_dt
            , psc.pk_product_sub_category
            , psc.fk_product_category
            , psc.sub_category_name
            , pc.pk_product_category
            , pc.name_product_category
        from product_details as pd
        left join product_sub_category as psc            
            on pd.fk_product_sub_category = psc.pk_product_sub_category
        left join product_category as pc
            on psc.fk_product_category = pc.pk_product_category
    )

    , deduplication as (
        select
            *
            , row_number() over (
                partition by pk_product_category, pk_product_sub_category
                order by pk_product
            ) as row_num
        from products
        qualify row_num = 1
    )

select *
from deduplication
