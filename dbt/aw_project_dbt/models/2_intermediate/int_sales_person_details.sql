with
    person as (
        select
            pk_person
            , person_type
            , name_style
            , title_name
            , first_name
            , middle_name
            , last_name
            , suffix
        from {{ ref('stg_mssql__person') }}
    )

    , sales_person as (
        select
            pk_vendor
            , fk_territory_sales
            , sales_yearly_quota
            , bonus_due
            , commission_sales_pct
            , sales_year_to_date
            , sales_last_year
        from {{ ref('stg_mssql__sales_person') }}
    )

    , sales_person_details as (
        select
            person.pk_person
            , person.person_type
            , person.name_style
            , person.title_name
            , person.first_name
            , person.middle_name
            , person.last_name
            , person.suffix
            , sales_person.fk_territory_sales
            , sales_person.sales_yearly_quota
            , sales_person.bonus_due
            , sales_person.commission_sales_pct
            , sales_person.sales_year_to_date
            , sales_person.sales_last_year

        from sales_person
        left join person
            on sales_person.pk_vendor = person.pk_person
    )

select * 
from sales_person_details
