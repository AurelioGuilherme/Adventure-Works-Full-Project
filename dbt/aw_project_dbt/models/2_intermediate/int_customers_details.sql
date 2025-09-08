with
    customers as (
        select
            pk_customer
            , fk_person
            , fk_store
            , fk_territory_sales
        from {{ ref('stg_mssql__customer') }}
    )

    , person as (
        select
            pk_person
            , person_type
            , name_style
            , title_name
            , first_name
            , middle_name
            , last_name
            , suffix
            , email_promotion
        from {{ ref('stg_mssql__person') }}
    )

    , store as (
        SELECT
            pk_store
            , name_store
        FROM {{ ref('stg_mssql__store') }}

    )

    , customers_details as (
        select
            customers.pk_customer
            , customers.fk_person
            , customers.fk_store
            , customers.fk_territory_sales
            , person.person_type
            , person.name_style
            , person.title_name
            , person.first_name
            , person.middle_name
            , person.last_name
            , person.suffix
            , person.email_promotion
            , store.pk_store
            , store.name_store
        from customers
        inner join person
            on customers.fk_person = person.pk_person
        inner join store
            on customers.fk_store = store.pk_store
    )

select *
from customers_details
