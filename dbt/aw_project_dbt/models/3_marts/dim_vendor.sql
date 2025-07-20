with 
    sales_person as (
        select
            pk_person
            , person_type
            , first_name
            , last_name
            , first_name || ' ' || last_name as full_name
            , fk_territory_sales
            , sales_yearly_quota
            , bonus_due
            , commission_sales_pct
            , sales_year_to_date
            , sales_last_year
        from {{ ref('int_sales_person_details') }}
    )

    -- adicionando chave surrogada
    , generate_sk as (
        select
            {{  dbt_utils.generate_surrogate_key([
                'pk_person'
                ])  
            }} as sk_vendor
                  , pk_person as pk_vendor
                  , full_name
                  , fk_territory_sales
                  , sales_yearly_quota
                  , bonus_due
                  , commission_sales_pct
                  , sales_year_to_date
                  , sales_last_year
        from sales_person
    )

select *
from generate_sk
