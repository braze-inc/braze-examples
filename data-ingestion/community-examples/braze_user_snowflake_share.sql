{{
    config(
        materialized='incremental',
        full_refresh=false
    )
}}

{#- Check if the table already exists. -#}
{%- set target_relation = adapter.get_relation(
      database=this.database,
      schema=this.schema,
      identifier=this.name) -%}
{%- set table_exists=target_relation is not none -%}

{#- Below, input the name of the model that has your user data. This model MUST include the following fields: -#}
{#- external_id: This is the unique identifier for your user. Source data should be one row per user. -#}
{#- updated_at: This field is the last time the data was updated in your model. -#}
{% set source = ref('YOUR_USER_SOURCE_MODEL') %}

{#- If there are any other fields that you want to exclude from your feed, list them below. Do not remove external_id or updated_at. -#}
{% set exclude_columns = ['external_id','updated_at'] %}
{% set col_string = dbt_utils.star(source, except=exclude_columns) | replace('"','') | replace(' ','') | replace('\n','') | lower %}
{% set colarr = col_string.split(',') %}

with 

-- Below converts your source model to a json object
user_source_obj as (
  select
    external_id
    , object_construct(
    {% for col in colarr -%}
      {% if col != colarr[0] %}, {% endif %}'{{col}}', {{col}}
    {% endfor %}
    ) as payload
    , updated_at
  from {{ source }}
)

{% if table_exists %}
-- Below pulls in existing records and flattens them
, loaded_user_data as (
  select u.external_id, v.key::varchar as key, v.value::variant as value, u.updated_at
  from {{this}} u,
  table(flatten(input => u.payload)) v
  qualify row_number() over (partition by u.external_id, v.key order by u.updated_at desc) = 1
)
{% endif %}

-- Below flattens your existing records
, current_values as (
  select u.external_id, v.key::varchar as key, v.value::variant as value, u.updated_at
  from user_source_obj u,
  table(flatten(input => u.payload)) v
)

-- Compares the most recent records currently transmitted to Braze, by key and external_id
-- If the new data is different from the existing data, that data will be transmitted
-- Only the individual cells that have changed will be sent, or external_ids that are completely new
select 
    n.external_id
    , object_agg(n.key, n.value) as payload
    , max(n.updated_at) as updated_at
from current_values n
{% if table_exists %}
    left join loaded_user_data o on n.external_id = o.external_id and n.key = o.key
where ifnull(n.value::string, '|') != ifnull(o.value::string, '|')
{% endif %}
group by 1
