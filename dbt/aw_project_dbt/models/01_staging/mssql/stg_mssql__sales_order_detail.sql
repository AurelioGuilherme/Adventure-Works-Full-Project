with
    sales_order_detail as (
        select 
            cast(SalesOrderID as int) as fk_sales_order
            , cast(SalesOrderDetailID as int) as pk_sales_order_detail
            , cast(CarrierTrackingNumber as varchar) as shipment_tracking_number
            , cast(OrderQty as int) as order_qty
            , cast(ProductID as int) as fk_product
            , cast(SpecialOfferID as int) as fk_special_offer
            , cast(UnitPrice as number(16, 4)) as unit_price
            , cast(UnitPriceDiscount as number(16, 4)) as unit_price_discount
            , cast(LineTotal as number(16, 4)) as sub_total_sale
            , cast(rowguid as varchar) as rowguid
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_sales_salesorderdetail' ) }}     
    )

select *
from sales_order_detail
