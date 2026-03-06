/*
    Safe division that returns null instead of erroring on divide-by-zero.
    
    Usage: {{ safe_divide('numerator', 'denominator') }}
    Output: case when denominator = 0 or denominator is null then null 
            else numerator / denominator end
*/

{% macro safe_divide(numerator, denominator, decimal_places=4) %}
    case 
        when {{ denominator }} = 0 or {{ denominator }} is null then null
        else round({{ numerator }}::decimal(18,6) / {{ denominator }}::decimal(18,6), {{ decimal_places }})
    end
{% endmacro %}
