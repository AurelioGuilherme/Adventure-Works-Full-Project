with
    vendor as (
        select
            cast(BusinessEntityID as int) as pk_vendor
            , cast(TerritoryID as int) as fk_territory_sales
            , cast(SalesQuota as numeric(16, 4)) as sales_yearly_quota
            , cast(Bonus as numeric(16, 4)) as bonus_due
            , cast(CommissionPct as numeric(6, 4)) as commission_sales_pct
            , cast(SalesYTD as numeric(16, 4)) as sales_year_to_date
            , cast(SalesLastYear as numeric(16, 4)) as sales_last_year
            , cast(rowguid as string) as rowguid
            , cast(ModifiedDate as date) as modified_date_dt
        from {{ source( 'source_aw_mssql', 'delta_raw_db_data_sales_salesperson' ) }}     
    )

select *
from vendor
