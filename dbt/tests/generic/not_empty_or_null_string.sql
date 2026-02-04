{# This is a generic data test to check that string columns are not empty #}

{% test not_empty_or_null_string(model, column_name) %}

    select *
    from {{ model }}
    where trim({{ column_name}}) = '' or {{ column_name }} IS NULL
{% endtest %}