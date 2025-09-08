with
    store as (
        select
            cast(BusinessEntityID as int) as pk_store
            , cast(Name as string) as name_store
            , cast(SalesPersonID as int) as fk_vendor
            , cast(Demographics as string) as demographics
            , cast(rowguid as string) as rowguid
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_sales_store' ) }}     
    )

select *
from store
