with 
    purchase_order_header as (
        select 
            cast(PurchaseOrderID as int) as pk_purchase_order
            , cast(RevisionNumber as int) as fk_purchase_order_detail
            , cast(Status as int) as due_date_dt
            , cast(EmployeeID as int) as order_qty
            , cast(VendorID as int) as product_id
            , cast(ShipMethodID as int) as unit_price
            , cast(OrderDate as date) as line_total
            , cast(ShipDate as date) as received_qty 
            , cast(SubTotal as numeric(18, 4)) as rejected_qty
            , cast(TaxAmt as numeric(18, 4)) as stocked_qty  
            , cast(Freight as numeric(18, 4)) as freight
            , cast(TotalDue as numeric(18, 4)) as total_due
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_api', 'delta_raw_api_data_purchaseorderheader' ) }}
    )

select *
from purchase_order_header
