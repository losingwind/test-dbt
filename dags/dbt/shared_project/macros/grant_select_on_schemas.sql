{% macro grant_select_on_schemas(users) %}
    {% if target.name == 'PROD' %}
        grant usage on schema {{ target.schema }} to {{ users|join(',') }};
        grant select on all tables in schema {{ target.schema }} to {{ users|join(',') }};
    {% else %}
        select 1; -- hooks will error if they don't have valid SQL in them, this handles that!
    {% endif %}
{% endmacro %}
