with
    product_inventory_details as (
        select
            cast(ProductID as int) as fk_product
            , cast(LocationID as int) as fk_stock_location
            , cast(Shelf as varchar) as compartment
            , cast(Bin as int) as container_in_inventory
            , cast(Quantity as int) as quantity
            , cast(rowguid as varchar) as rowguid
            , cast(ModifiedDate as date) as modified_date
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_production_productinventory' ) }}     
    )

select *
from product_inventory_details
