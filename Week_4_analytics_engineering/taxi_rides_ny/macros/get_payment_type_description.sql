{#
    This macro returns the description of the payment_type 
#}

{% macro get_payment_type_description(payment_type) -%}
{#api.Column.translate_type("integer") là một phần của API dbt, giúp dịch kiểu dữ liệu SQL tương ứng cho từng Data Warehouse (DWH).#}
    case {{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }}  
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        else 'EMPTY'
    end

{%- endmacro %}