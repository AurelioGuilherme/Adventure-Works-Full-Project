with
    adress as (
        select
            cast(AddressID as int) as pk_address
            , cast(AddressLine1 as string) as address_line_1
            , cast(AddressLine2 as string) as address_line_2
            , cast(City as string) as city
            , cast(StateProvinceID as int) as fk_state_province
            , cast(PostalCode as string) as postal_code
            , cast(SpatialLocation as string) as spatial_location
            , cast(rowguid as string) as rowguid
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_person_address' ) }}     
    )

select *
from adress
