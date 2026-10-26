{% test unique_combination(model, combination_of_columns) %}
select
    {{ combination_of_columns | join(', ') }},
    count(*) as num_records
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1
{% endtest %}
