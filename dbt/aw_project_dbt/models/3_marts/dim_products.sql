with
    -- Produtos
    products as (
        select
            pk_product
            , name_product
            , product_category_name
            , sub_category_name
            , product_color
            , gender_category_product
            , product_line
            , product_class
            , standard_cost
            , selling_price
            , is_final_product
            , is_manufactured
        from {{ ref('int_product') }}
    )

    -- adicionando chave surrogada
    , generate_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['pk_product']) }} as sk_products
            , pk_product
            , name_product
            , product_category_name
            , sub_category_name
            , product_color
            , gender_category_product
            , product_line
            , product_class
            , standard_cost
            , selling_price
            , is_final_product
            , is_manufactured
        from products
    )

select *
from generate_sk