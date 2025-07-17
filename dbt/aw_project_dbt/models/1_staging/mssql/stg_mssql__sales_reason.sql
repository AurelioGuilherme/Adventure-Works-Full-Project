with
    sales_reason as (
        select 
            cast(SalesReasonID as int) as pk_sales_reason
            , cast(Name as string) as reason_name
            , cast(ReasonType as string) as reason_type
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_sales_salesreason' ) }}     
    )

select *
from sales_reason
