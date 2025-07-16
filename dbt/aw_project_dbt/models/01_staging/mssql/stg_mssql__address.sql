with
    adress as (
        select
            cast(AddressID as int) as pk_address
            , cast(AddressLine1 as varchar) as address_line_1
            , cast(AddressLine2 as varchar) as address_line_2
            , cast(City as varchar) as city
            , cast(StateProvinceID as int) as fk_state_province
            , cast(PostalCode as varchar) as
            , cast(SpatialLocation as geography) as spatial_location
            , cast(rowguid as varchar) as rowguid
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_person_address' ) }}     
    )

select *
from adress
