with
    prod_category as (
        select
            cast(ProductCategoryID as int) as pk_product_category
            , cast(Name as string) as name_product_category
            , cast(rowguid as string) as rowguid
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_production_productcategory' ) }}     
    )

select *
from prod_category
