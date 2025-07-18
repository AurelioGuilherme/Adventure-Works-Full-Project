with
    product_location as (
        select
            cast(LocationID as int) as pk_stock_location
            , cast(Name as string) as name_stock_location
            , cast(CostRate as numeric(18, 4)) as cost_rate_stock
            , cast(Availability as int) as total_capacity_stock
            , cast(ModifiedDate as date) as modified_date
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_production_location' ) }}     
    )

select *
from product_location
