with
    territory as (
        select
            pk_address
            , full_address
            , city
            , name_province
            , name_state
            , country_region_code
            , territory_name
            , country_code
            , territory_group
        from {{ ref('int_territory') }}
    )

    , generate_sk as (
        select
            {{  dbt_utils.generate_surrogate_key([
                'pk_address'
                ])  
            }} as sk_address
                  , pk_address
                  , full_address
                  , city
                  , name_province
                  , name_state
                  , country_region_code
                  , territory_name
                  , country_code
                  , territory_group
        from territory
    )

select * 
from generate_sk