with 
    state_province as (
        select 
            pk_state_province
            , state_province_code
            , country_region_code
            , flag_state_or_province
            , name_state_province
            , fk_territory_sales
        from {{ ref("stg_mssql__state_province") }}
    )

    , sales_territory as (
        select
            pk_territory_sales
            , territory_name
            , country_code
            , territory_group
            , sales_year_to_date
            , sales_last_year
            , territory_cost_year_to_date
            , territory_cost_last_year
        from {{ ref("stg_mssql__sales_territory") }} 
    )

    , adress as (
        select 
            pk_address
            , address_line_1
            , address_line_2
            , city
            , fk_state_province
            , postal_code
        from {{ ref("stg_mssql__address") }} 
    )

    , territory as (
        select
            sp.pk_state_province
            , sp.state_province_code
            , sp.country_region_code
            , sp.flag_state_or_province
            , sp.name_state_province
            , sp.fk_territory_sales
            , st.pk_territory_sales
            , st.territory_name
            , st.country_code
            , st.territory_group
            , st.sales_year_to_date
            , st.sales_last_year
            , st.territory_cost_year_to_date
            , st.territory_cost_last_year
            , ad.pk_address
            , ad.address_line_1
            , ad.address_line_2
            , ad.city
            , ad.fk_state_province
            , ad.postal_code
        from  sales_territory as st
        left join state_province as sp
            on st.pk_territory_sales = sp.fk_territory_sales
        left join adress as ad
            on sp.pk_state_province = ad.fk_state_province
    )

    , deduplication as (
        select
            *
            , row_number() over (
                partition by pk_address, pk_state_province, pk_territory_sales
                order by pk_address
            ) as row_num
        from territory
        qualify row_num = 1
    )

select *
from deduplication