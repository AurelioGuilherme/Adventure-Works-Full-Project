with
    person_address as (
        select
            cast(BusinessEntityID as int) as fk_person
            , cast(AddressID as int) as fk_address
            , cast(AddressTypeID as int) as fk_address_type
            , cast(rowguid as string)
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_person_businessentityaddress' ) }}     
    )

select *
from person_address
