with
    sales_territory as (
        select
            cast(TerritoryID as int) as pk_territory_sales
            , cast(Name as varchar) as territory_name
            , cast(CountryRegionCode as varchar) as country_code
            , cast(Group as varchar) as territory_group
            , cast(SalesYTD as number(18, 4)) as sales_year_to_date
            , cast(SalesLastYear as number(18, 4)) as sales_last_year
            , cast(CostYTD as number(18, 4)) as territory_cost_year_to_date
            , cast(CostLastYear as number(18, 4)) as territory_cost_last_year
            , cast(rowguid as varchar) as rowguid
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_sales_salesterritory' ) }}     
    )

select *
from sales_territory
