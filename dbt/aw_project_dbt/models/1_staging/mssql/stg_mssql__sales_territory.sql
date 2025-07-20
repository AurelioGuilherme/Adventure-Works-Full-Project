with
    sales_territory as (
        select
            cast(TerritoryID as int) as pk_territory_sales
            , cast(Name as string) as territory_name
            , cast(CountryRegionCode as string) as country_code
            , cast(Group as string) as territory_group
            , cast(SalesYTD as numeric(18, 4)) as sales_year_to_date
            , cast(SalesLastYear as numeric(18, 4)) as sales_last_year
            , cast(CostYTD as numeric(18, 4)) as territory_cost_year_to_date
            , cast(CostLastYear as numeric(18, 4)) as territory_cost_last_year
            , cast(rowguid as string) as rowguid
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_sales_salesterritory' ) }}     
    )

select *
from sales_territory
