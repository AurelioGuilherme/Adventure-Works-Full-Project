with
    credit_card as (
        select 
            cast(CreditCardID as int) as pk_credit_card
            , cast(CardType as varchar) as card_type
            , cast(CardNumber as varchar) as card_number
            , cast(ExpMonth as int) as expiration_month
            , cast(ExpYear as int) as expiration_year 
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_sales_creditcard' ) }}     
    )

select *
from credit_card
