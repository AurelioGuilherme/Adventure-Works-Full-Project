with
    prod_category as (
        select
            cast(ProductCategoryID as ) as pk_product_category
            , cast(Name as varchar) as name_product_category
            , cast(rowguid as varchar) as rowguid
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_production_productcategory' ) }}     
    )

select *
from prod_category
