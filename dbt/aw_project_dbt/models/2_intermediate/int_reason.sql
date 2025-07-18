with
    sales_reason_headers as (
        select 
            pk_sales_reason
            , fk_sales_order
        from {{ ref("stg_mssql__sales_reason_header") }}
    )

    , sales_reason as (
        select
            fk_sales_reason
            , reason_name
            , reason_type 
        from {{ ref("stg_mssql__sales_reason") }}
    )

    , reason as (
        select
            srh.pk_sales_reason
            , srh.fk_sales_order
            , sr.fk_sales_reason
            , sr.reason_name
            , sr.reason_type
        from sales_reason_headers as srh
        left join sales_reason as sr
            on srh.pk_sales_reason = sr.fk_sales_reason
    )

    , deduplication as (
        select
            *
            , row_number() over (
                partition by fk_sales_order, pk_sales_reason, fk_sales_reason
                order by fk_sales_order
            ) as row_num
        from reason
        qualify row_num = 1
    )

select *
from deduplication
