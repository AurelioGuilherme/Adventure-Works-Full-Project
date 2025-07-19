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
            , sell_start_date_dt
            , sell_end_date_dt
            , discontinued_date_dt
        from {{ ref("stg_mssql__product") }}
    )

    , product_category_base as (
        select
            pk_product_category
            , name_product_category
        from {{ ref("stg_mssql__product_category") }}
    )

    , product_sub_category_base as (
        select
            pk_product_sub_category
            , fk_product_category
            , sub_category_name
        from {{ ref("stg_mssql__product_sub_category") }}
    )

    , max_ids as (
        select 
            max(product_category_base.pk_product_category) + 1 as not_classified_category_id
            , max(product_sub_category_base.pk_product_sub_category) + 1 as not_classified_subcategory_id

        from product_category_base
        cross join product_sub_category_base
    )

    , product_category as (
        select 
            * 
        from product_category_base
        union all
        select 
            not_classified_category_id
            , 'Not Classified'
        from max_ids
    )

    , product_sub_category as (
        select 
            * 
        from product_sub_category_base
        union all
        select
            not_classified_subcategory_id
            , not_classified_category_id
            , 'Not Classified'
        from max_ids
    )

    , products_coalesce as (
        select
            product_details.*
            , coalesce(psc.pk_product_sub_category, max_ids.not_classified_subcategory_id) as pk_product_sub_category
            , coalesce(psc.sub_category_name, 'Not Classified') as sub_category_name
            , coalesce(psc.fk_product_category, max_ids.not_classified_category_id) as fk_product_category
            , coalesce(pc.pk_product_category, max_ids.not_classified_category_id) as pk_product_category
            , coalesce(pc.name_product_category, 'Not Classified') as name_product_category
        from product_details
        cross join max_ids
        left join product_sub_category as psc            
            on product_details.fk_product_sub_category = psc.pk_product_sub_category
        left join product_category as pc
            on coalesce(psc.fk_product_category, max_ids.not_classified_category_id) = pc.pk_product_category
    )

    , deduplication as (
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
            , sell_start_date_dt
            , sell_end_date_dt
            , discontinued_date_dt
            , pk_product_sub_category
            , sub_category_name
            , fk_product_category
            , pk_product_category
            , name_product_category
            , row_number() over (
                partition by pk_product_category, pk_product_sub_category
                order by pk_product
            ) as row_num
        from products_coalesce
        qualify row_num = 1
    )

select *
from deduplication
