with 
    customer as (
        select
            pk_customer
            , fk_person
            , fk_territory_sales
            , person_type
            , name_style
            , first_name
            , middle_name
            , last_name
            , email_promotion
        from {{ ref('int_customers_details') }}
        where person_type in ('IN', 'SC')
    )

    -- removendo os dados duplicados
    , person_address_ranked as (
        select
            fk_person
            , fk_address
            , row_number() over (partition by fk_person order by fk_address) as rn
        from {{ ref('stg_mssql__person_address') }}
    )

    , person_address as (
        select
            fk_person
            , fk_address
        from person_address_ranked
        where rn = 1
    )
    
    , territory as (
        select
            *
        from {{ ref('int_territory') }}
    )

    , fact_customers as (
        select
            customer.pk_customer 
            , customer.fk_person 
            , customer.fk_territory_sales
            , customer.person_type
            , customer.first_name
            , customer.middle_name
            , customer.last_name
            , customer.name_style
            , customer.email_promotion

            -- Nome completo
            , case
                -- nome estilo oriental (Sobrenome, Nome)
                when customer.name_style = true then customer.last_name || ', ' || customer.first_name
                -- nome estilo ocidental (Nome nome_do_meio sobrenome)
                else customer.first_name || ' ' || coalesce(customer.middle_name || ' ', '') || customer.last_name
            end as full_name

            -- Dados de região
            , territory.territory_name
            , territory.country_code
            , territory.territory_group
            , territory.name_state_province
            , territory.city
        from customer
        left join person_address
            on customer.fk_person = person_address.fk_person
        left join territory
            on person_address.fk_address = territory.pk_address
    )

    -- adicionando chave surrogada
    , generate_sk as (
        select
            {{  dbt_utils.generate_surrogate_key([
                'pk_customer'
                ])  
            }} as sk_customer
                  , pk_customer 
                  , fk_person 
                  , fk_territory_sales
                  , person_type
                  , first_name
                  , middle_name
                  , last_name
                  , name_style
                  , email_promotion
                  , full_name
                  , territory_name
                  , country_code
                  , territory_group
                  , name_state_province
                  , city
        from fact_customers
    )

select *
from generate_sk
