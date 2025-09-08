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
            , name_store
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

    , last_purchase as (
        select 
            fk_customer
            , max(order_date_dt) as last_order_date
        from {{ ref('int_orders')}}
        group by fk_customer
    )

    , max_sale_date as (
        select max(order_date_dt) as max_order_date
        from {{ ref('int_orders') }}
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
            , customer.name_store

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

    , purchase_count as (
        select
            fk_customer
            , count(distinct pk_sales_order) as total_orders
        from {{ ref('int_orders') }}
        group by fk_customer
    ) 

    , customer_with_last_purchase as (
            select
                fc.*
                , lp.last_order_date
                , datediff(day, lp.last_order_date, msd.max_order_date) as days_since_last_purchase
                , pc.total_orders
            from fact_customers fc
            left join last_purchase lp on fc.pk_customer = lp.fk_customer
            left join purchase_count pc on fc.pk_customer = pc.fk_customer
            cross join max_sale_date as msd
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
                  , name_store
                  , case
                        when email_promotion = 0 then 'Não Optante'
                        when email_promotion = 1 then 'Optante AW'
                        when email_promotion = 2 then 'Optante AW + Parceiros'
                        else 'Desconhecido'
                    end as email_promotion_description
                  , full_name
                  , territory_name
                  , country_code
                  , territory_group
                  , name_state_province
                  , city
                  , last_order_date
                  , days_since_last_purchase
                  , case
                        when days_since_last_purchase <= 30 then '0-30 dias'
                        when days_since_last_purchase <= 60 then '31-60 dias'
                        when days_since_last_purchase <= 90 then '61-90 dias'
                        when days_since_last_purchase <= 120 then '91-120 dias'
                        when days_since_last_purchase <= 150 then '121-150 dias'
                        when days_since_last_purchase <= 200 then '151-200 dias'
                        when days_since_last_purchase <= 360 then '201-360 dias'
                        when days_since_last_purchase > 360 then 'Mais de 360 dias'
                        else 'Sem registro de compra'
                  end as purchase_recency_class
                  , case
                        when total_orders = 1 then true
                        else false
                    end as bought_once
                  , case
                        when bought_once = True then 'Compra única'
                        else 'Recorrente'
                    end as bought_once_description

        from customer_with_last_purchase
    )

select *
from generate_sk
