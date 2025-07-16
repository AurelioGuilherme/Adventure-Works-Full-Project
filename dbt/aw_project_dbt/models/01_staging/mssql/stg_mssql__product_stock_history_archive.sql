with
    stock_history_archive as (
        select  
            cast(TransactionID as int) as pk_stock_transaction
            , cast(ProductID as int) as fk_product
            , cast(ReferenceOrderID as int) as reference_order
            , cast(ReferenceOrderLineID as int) as reference_order_line
            , cast(TransactionDate as date) as transaction_date_dt
            , cast(TransactionType as varchar) as transaction_type
            , cast(Quantity as int) as stock_quantity
            , cast(ActualCost as numeric(18, 4)) as actual_cost_stock
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_production_transactionhistoryarchive' ) }}     
    )

select *
from stock_history_archive
