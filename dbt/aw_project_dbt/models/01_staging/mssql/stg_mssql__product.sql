with
    product as (
        select
            cast(ProductID as int) as pk_product
            , cast(Name as varchar) as name_product
            , cast(ProductNumber as varchar) as serial_product_number
            , cast(MakeFlag as boolean) as is_manufactured
            , cast(FinishedGoodsFlag as boolean) as is_final_product
            , cast(Color as varchar) as product_color
            , cast(SafetyStockLevel as int) as safety_stock_level
            , cast(ReorderPoint as int) as minimal_stock_level
            , cast(StandardCost as numeric(18, 4)) as standard_cost
            , cast(ListPrice as numeric(18, 4)) as selling_price
            , cast(Size as varchar) as product_size
            , cast(SizeUnitMeasureCode as varchar) as product_unit_size
            , cast(WeightUnitMeasureCode as varchar) as product_unit_weight
            , cast(Weight as numeric(6, 2)) as product_weight
            , cast(DaysToManufacture as int) as days_to_manufacture
            , cast(ProductLine as varchar) as product_line
            , cast(Class as varchar) as product_class
            , cast(Style as varchar) as gender_category_product
            , cast(ProductSubcategoryID as int) as fk_product_sub_category
            , cast(ProductModelID as int) as fk_product_model
            , cast(SellStartDate as date) as sell_start_date_dt
            , cast(SellEndDate as date) as sell_end_date_dt
            , cast(DiscontinuedDate as date) as discontinued_date_dt
            , cast(rowguid as varchar) as rowguid
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_production_product' ) }}     
    )

select *
from product
