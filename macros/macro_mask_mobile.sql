{% macro mask_mobile(mobile_column) %}
    CONCAT('XXXXXXX', RIGHT({{ mobile_column }}, 3))
{% endmacro %}

