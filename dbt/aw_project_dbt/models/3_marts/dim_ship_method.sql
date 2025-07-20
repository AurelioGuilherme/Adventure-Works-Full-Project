with 
    ship_method as (
        select 
            pk_ship_method
            , shipping_name
            , minimum_shipping_charge
            , shipping_cost_rate 
        from {{ ref('stg_mssql__ship_method') }}
    )

    , generate_sk as (
        select
            {{  dbt_utils.generate_surrogate_key([
                'pk_ship_method'
                ])  
            }} as sk_ship_method
                , pk_ship_method
                , shipping_name
                , shipping_cost_rate 
                , minimum_shipping_charge
        from ship_method
    )

select *
from generate_sk
