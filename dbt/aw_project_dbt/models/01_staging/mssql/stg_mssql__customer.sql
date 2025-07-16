with
    customer as (
        select
            cast(CustomerID as int) as pk_customer
            , cast(PersonID as int) as fk_person
            , cast(StoreID as int) as fk_store
            , cast(TerritoryID as int) as fk_territory_sales
            , cast(AccountNumber as varchar) as account_number
            , cast(rowguid as varchar) as rowguid
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_sales_customer' ) }}     
    )

select *
from customer
