with
    inventory as (
        select
            fk_product 
            , fk_stock_location
            , compartment
            , container_in_inventory
            , quantity
        from {{ ref("stg_mssql__product_inventory_details") }}
    )

    , product_location as (
        select
            pk_stock_location
            , name_stock_location
            , cost_rate_stock
            , total_capacity_stock
        from {{ ref("stg_mssql__product_location") }}
    )

    , inventory_details as (
        select
            invd.fk_product
            , invd.fk_stock_location
            , invd.compartment
            , invd.container_in_inventory
            , invd.quantity
            , pl.pk_stock_location
            , pl.name_stock_location
            , pl.cost_rate_stock
            , pl.total_capacity_stock
        from inventory as invd
        left join product_location as pl
            on invd.fk_stock_location = pl.pk_stock_location
    )

    , deduplication as (
        select
            *,
            row_number() over (
                partition by fk_product, pk_stock_location
                order by fk_stock_location
            ) as row_num
        from inventory_details
        qualify row_num = 1
    )

select *
from deduplication
