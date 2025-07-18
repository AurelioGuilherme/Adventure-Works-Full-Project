with 
    purchase_order_header as (
        select 
            cast(PurchaseOrderID as int) as pk_purchase_order
            , cast(RevisionNumber as int) as revision_number
            , cast(Status as int) as status_code
            , cast(EmployeeID as int) as fk_employee
            , cast(VendorID as int) as fk_vendor
            , cast(ShipMethodID as int) as fk_ship_method
            , cast(OrderDate as date) as order_date_dt
            , cast(ShipDate as date) as ship_date_dt
            , cast(SubTotal as numeric(18, 4)) as sub_total
            , cast(TaxAmt as numeric(18, 4)) as tax_amt  
            , cast(Freight as numeric(18, 4)) as freight
            , cast(TotalDue as numeric(18, 4)) as total_due
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_api', 'delta_raw_api_data_purchaseorderheader' ) }}
    )

select *
from purchase_order_header
