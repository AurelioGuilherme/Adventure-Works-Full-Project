{% snapshot snp_status_sales %}

{{
    config(
        target_schema='snapshots'
        , unique_key='pk_sales_order_detail'
        , strategy='check'           
        , check_cols=['sales_status', 'ship_date_dt', 'due_date_dt']
    )
}}

-- Snapshot do status do pedido para monitorar mudanças ao longo do tempo
-- Status possíveis:
-- 1 = In process
-- 2 = Approved
-- 3 = Backordered
-- 4 = Rejected
-- 5 = Shipped
-- 6 = Cancelled

select
    pk_sales_order_detail
    , fk_sales_order
    , fk_customer
    , order_date_dt
    , due_date_dt
    , ship_date_dt
    , sales_status
from {{ ref('int_orders') }}

{% endsnapshot %}
