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


    -- Mapeamento de product_line
    , map_product_line as (
        select 
            * 
        from (
            values
                ('R', 'Road')
                , ('M', 'Mountain')
                , ('T', 'Touring')
                , ('S', 'Standard')
        ) as t(code, description)
    )

    -- Mapeamento de product_class
    , map_product_class as (
        select 
            * 
        from (
            values
                ('H', 'High')
                , ('M', 'Medium')
                , ('L', 'Low')
        ) as t(code, description)
    )
    
    -- adicionando chave surrogada
    , joined as (
        select
            {{ dbt_utils.generate_surrogate_key(['p.pk_product']) }} as sk_products
            , p.*
            , p.selling_price - p.standard_cost as margin_amount
            , coalesce(pl.description, 'Unknown') as product_line_name
            , coalesce(pc.description, 'Unknown') as product_class_name
            , CASE 
                WHEN p.is_manufactured = True THEN 'Fabricado' 
                WHEN p.is_manufactured = False THEN 'Revendido'
                ELSE 'Desconhecido'
            END as manufactured_description
            , CASE 
                WHEN p.gender_category_product = 'M' THEN 'Masculino' 
                WHEN p.gender_category_product = 'F' THEN 'Feminino'
                WHEN p.GENDER_CATEGORY_PRODUCT = 'U' THEN 'Unissex'
                ELSE 'Não Especificado'
            END as gender_category_description
        from products p
        left join map_product_line pl
            on trim(upper(p.product_line)) = pl.code
        left join map_product_class pc
            on trim(upper(p.product_class)) = pc.code
    )

select *
from joined
