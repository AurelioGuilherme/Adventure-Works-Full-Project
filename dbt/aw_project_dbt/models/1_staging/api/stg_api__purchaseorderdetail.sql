with 
    purchase_order_detail as (
        select 
            cast(PurchaseOrderID as int) as fk_purchase_order
            , cast(PurchaseOrderDetailID as int) as pk_purchase_order_detail
            , cast(DueDate as date) as due_date_dt
            , cast(OrderQty as int) as order_qty
            , cast(ProductID as int) as product_id
            , cast(UnitPrice as numeric(18, 4)) as unit_price
            , cast(LineTotal as numeric(18, 4)) as line_total
            , cast(ReceivedQty as int) as received_qty 
            , cast(RejectedQty as int) as rejected_qty
            , cast(StockedQty as int) as stocked_qty  
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_api', 'delta_raw_api_data_purchaseorderdetail' ) }}
    )

select *
from purchase_order_detail