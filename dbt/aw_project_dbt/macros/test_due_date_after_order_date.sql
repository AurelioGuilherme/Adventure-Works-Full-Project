{% test due_date_after_order_date(model, column_name) %}
    select *
    from {{ model }}
    WHERE due_date_dt < order_date_dt
{% endtest %}
