{% macro convert_to_quarter(date_column) %}
    CAST(EXTRACT(YEAR FROM {{ date_column }}) AS STRING) || ' Q' || CAST(EXTRACT(QUARTER FROM {{ date_column }}) AS STRING)
{% endmacro %}