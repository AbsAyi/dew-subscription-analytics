/*
    Converts Stripe-style cent amounts to dollars.
    Stripe stores all monetary values in the smallest currency unit (cents for USD).
    
    Usage: {{ cents_to_dollars('amount') }}
    Output: (amount / 100.0)::decimal(10,2)
*/

{% macro cents_to_dollars(column_name) %}
    ({{ column_name }} / 100.0)::decimal(10,2)
{% endmacro %}
