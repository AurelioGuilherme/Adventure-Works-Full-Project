with
    product_sub_category as (
        select
            cast(ProductSubcategoryID as int) as pk_product_sub_category
            , cast(ProductCategoryID as int) as fk_product_category
            , cast(Name as varchar) as sub_category_name
            , cast(rowguid as varchar) as rowguid
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_production_productsubcategory' ) }}     
    )

select *
from product_sub_category




