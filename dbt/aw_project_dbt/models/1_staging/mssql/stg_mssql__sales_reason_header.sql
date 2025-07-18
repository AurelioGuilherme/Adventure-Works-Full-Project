with
    sales_reason_header as (
        select
            cast(SalesOrderID as int) as fk_sales_order
            , cast(SalesReasonID as int) as pk_sales_reason
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_sales_salesorderheadersalesreason' ) }}     
    )

select *
from sales_reason_header
