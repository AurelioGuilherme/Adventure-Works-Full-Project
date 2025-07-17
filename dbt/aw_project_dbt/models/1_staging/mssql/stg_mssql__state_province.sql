with
    state_province as (
        select
            cast(StateProvinceID as int) as pk_state_province
            , cast(StateProvinceCode as string) as state_province_code
            , cast(CountryRegionCode as string) as country_region_code
            , cast(IsOnlyStateProvinceFlag as boolean) as flag_state_or_province
            , cast(Name as string) as name_state_province
            , cast(TerritoryID as int) as fk_territory_sales
            , cast(rowguid as string) as rowguid
            , cast(ModifiedDate as date) as modified_date
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_person_stateprovince' ) }}     
    )

select *
from state_province
