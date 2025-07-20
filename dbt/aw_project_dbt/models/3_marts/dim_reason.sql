with
    sales_reason as (
        select
            fk_sales_reason
            , reason_name
            , reason_type 
        from {{ ref("stg_mssql__sales_reason") }}
    )

    , generate_sk as (
        select
            {{  dbt_utils.generate_surrogate_key([
                'fk_sales_reason'
                ])  
            }} as sk_reason
                , fk_sales_reason
                , reason_name
                , reason_type
        from sales_reason
    )

select *
from generate_sk
