select *
from {{ ref('int_product') }}
where fk_product_category is null
