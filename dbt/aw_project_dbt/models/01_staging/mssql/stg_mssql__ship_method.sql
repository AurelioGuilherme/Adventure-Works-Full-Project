with
    ship_method as (
        select 
            cast(ShipMethodID as int) as pk_ship_method
            , cast(Name as varchar) as shipping_name
            , cast(ShipBase as numeric(16, 4)) as minimum_shipping_charge
            , cast(ShipRate as numeric(16, 4)) as shipping_cost_rate
            , cast(rowguid as varchar) as rowguid
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_purchasing_shipmethod' ) }}     
    )

select *
from ship_method
