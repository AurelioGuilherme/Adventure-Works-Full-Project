with
    sales_order_header as (
        select 
            cast(SalesOrderID as int) as pk_sales_order
            , cast(RevisionNumber as int) as revision_number
            , cast(OrderDate as date) as order_date_dt
            , cast(DueDate as date) as due_date_dt
            , cast(Status as int) as sales_status
            , cast(OnlineOrderFlag as boolean) as online_order_flag
            , cast(SalesOrderNumber as varchar) as sales_order_number
            , cast(CustomerID as int) as fk_customer
            , cast(BillToAddressID as int) as fk_bill_to_address
            , cast(ShipToAddressID as int) as fk_ship_to_address
            , cast(ShipMethodID as int) as fk_ship_method
            , cast(SubTotal as numeric(18, 4)) as sales_sub_total
            , cast(TaxAmt as numeric(18, 4)) as sales_tax_amt
            , cast(Freight as numeric(18, 4)) as sales_freight
            , cast(TotalDue as numeric(18, 4)) as total_due
            , cast(rowguid as varchar) as row_guid
            , cast(ModifiedDate as date) as modified_date_dt
            , cast(ShipDate as date) as ship_date_dt
            , cast(PurchaseOrderNumber as varchar) as purchase_order_number
            , cast(AccountNumber as varchar) as account_number
            , cast(SalesPersonID as int) as fk_vendor
            , cast(TerritoryID as int) as fk_territory_sales
            , cast(CreditCardID as int) as fk_credit_card
            , cast(CreditCardApprovalCode as varchar) as credit_card_approval_code
            , cast(CurrencyRateID as int) as fk_currency_rate
            , cast(Comment as varchar) as comment_text
        from {{ source( 'source_aw_api', 'delta_raw_api_data_salesorderheader' ) }}     
    )

select *
from sales_order_header
