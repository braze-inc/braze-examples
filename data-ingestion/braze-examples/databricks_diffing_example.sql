-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Diffing Notebook
-- MAGIC Braze requires that only new data is pushed in to Braze. This is in part due to data points but also for actions such as triggering workflows when an attribute is added/changed.  
-- MAGIC This notebook simply converts your internal user model you'd like to replicate to Braze and manages the tracking of changes and the transformations to Braze's expected output format.  
-- MAGIC This notebook supports:  
-- MAGIC - Simply attributes - string, bool, numbers...
-- MAGIC - Arrays array creation, new item added, item removal, array deletion
-- MAGIC - Nested Custom Attributes - will update only the individual key/value pair that changed within the object rather than having to rewrite the object every time
-- MAGIC - Arrays of objects - item addition/removal, key/value based updates as with Nested Custom Attributes
-- MAGIC
-- MAGIC ## How to use this Notebook
-- MAGIC 1. Replace the variables \<schema_name\> and \<user_model\> with the schema and table you wish to treat as your internal "user" model. This can be a view, materialized view, or table. 
-- MAGIC 2. Define which columns/attributes are arrays, Nested Custom Attributes, and Arrays of Objects. This tells the pipeline to treat those attributes than more than just a simple string diff.
-- MAGIC   a. Replace the example value in the nested diffing cell (where it says REPLACE_EXAMPLE_VALUES_HERE)
-- MAGIC 3. Run this notebook on a schedule. This notebook works betwen maintaining a snapshot of the source user_table between each run. You can run this notebook as often as you like.
-- MAGIC 4. Connect Braze to the output table -- -- of this notebook.
-- MAGIC 5. Done!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UDF's
-- MAGIC Required for nested data diffing and Braze formatting.

-- COMMAND ----------

CREATE FUNCTION IF NOT EXISTS <schema_name>.array_diff(arr_1_str STRING, arr_2_str STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
  import json

  def array_diff(arr_1, arr_2):
    if arr_2 == None or arr_2 == []:
        return None
    removed = set(arr_1).difference(arr_2)
    added = set(arr_2).difference(arr_1)

    update_obj = {}
    if len(removed) > 0:
        update_obj['remove'] = list(removed)

    if len(added) > 0:
        update_obj['add'] = list(added)

    if update_obj == {}:
      # the arrays are the same, just in a different order
      return None

    return update_obj

  arr_1 = json.loads(arr_1_str)
  arr_2 = json.loads(arr_2_str)
  result = json.dumps(array_diff(arr_1, arr_2))

  return result
$$;

-- COMMAND ----------

CREATE FUNCTION IF NOT EXISTS <schema_name>.json_deep_diff(a STRING, b STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
import json
def deep_diff(obj_a, obj_b):
    keys = list(obj_a.keys())
    keys.extend(obj_b.keys())
    keys = list(set(keys))
    update_obj = {}
    for key in keys:
        val_a = obj_a.get(key)
        val_b = obj_b.get(key)
        if str(val_a) == str(val_b):
            continue
        if type(val_b) == type({}) and val_a != None:
            val_b = deep_diff(val_a, val_b)
        update_obj[key] = val_b
    return update_obj

obj_a = json.loads(a)
obj_b = json.loads(b)
diff = deep_diff(obj_a, obj_b)
return json.dumps(diff)
$$;

-- COMMAND ----------

CREATE FUNCTION IF NOT EXISTS <schema_name>.json_array_deep_diff(a STRING, b STRING, id_key STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
import json

def deep_diff(obj_a, obj_b):
    keys = list(obj_a.keys())
    keys.extend(obj_b.keys())
    keys = list(set(keys))
    update_obj = {}
    for key in keys:
        val_a = obj_a.get(key)
        val_b = obj_b.get(key)
        if str(val_a) == str(val_b):
            continue
        if type(val_b) == type({}) and val_a != None:
            val_b = deep_diff(val_a, val_b)
        update_obj[key] = val_b
    return update_obj


def arr_of_objs_diff(arr_a, arr_b, id_field):
    objs_a = {}
    for row in arr_a:
        key = row[id_field]
        objs_a[key] = row

    objs_b = {}
    for row in arr_b:
        key = row[id_field]
        objs_b[key] = row
    
    deletions = []
    to_delete = set(objs_a.keys()).difference(objs_b.keys())
    for key in to_delete:
        deletions.append({
            "__dollar__identifier_key": id_field,
            "__dollar__identifier_value": key
        })

    additions = []
    to_add = set(objs_b.keys()).difference(objs_a.keys())
    for key in to_add:
        additions.append(objs_b[key])

    updates = []
    overlap = set(objs_b.keys()).intersection(objs_a.keys())
    for key in overlap:
        diff = deep_diff(objs_a[key], objs_b[key])
        if diff == {}:
            continue
        updates.append({
            "__dollar__identifier_key": id_field,
            "__dollar__identifier_value": key,
            "__dollar__new_object": diff
        })

    update_obj = {}
    if len(deletions) > 0:
        update_obj['__dollar__remove'] = deletions

    if len(additions) > 0:
        update_obj['__dollar__add'] = additions

    if len(updates) > 0:
        update_obj['__dollar__update'] = updates

    return update_obj

data_a = json.loads(a)
data_b = json.loads(b)
diff = arr_of_objs_diff(data_a, data_b, id_key)
return json.dumps(diff)
$$;

-- COMMAND ----------

CREATE FUNCTION IF NOT EXISTS <schema_name>.json_from_items(items_string STRING, additional_items_string STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
import json
items = json.loads(items_string)
new_dict = {}
for item in items:
  if item.get('value') == None:
    new_dict[item['key']] = None
  elif item.get('datatype') in ['array', 'json', 'json_array']:
    val = item['value'].replace("__quote__", '"')
    data = json.loads(val)
    new_dict[item['key']] = data
  else:
    new_dict[item['key']] = item['value']

additional_items = json.loads(additional_items_string)
new_dict = new_dict | additional_items
return json.dumps(new_dict)
$$;


-- COMMAND ----------

-- create a blank of the checkpoint table if it doesn't exist. For the first run of the table.
create table if not exists <schema_name>.<user_model>_diff_checkpoint as 
  select * from <schema_name>.<user_model> limit 0;

-- string diff table
create table if not exists <schema_name>.<user_model>_diff(
  user_id string,
  key string,
  old_value string,
  new_value string,
  created_ts timestamp
);

-- granular diff table
create table if not exists <schema_name>.<user_model>_granular_diff(
  user_id string,
  key string,
  old_value string,
  new_value string,
  diff string,
  datatype string,
  created_ts timestamp
);

-- braze updates output table
create table if not exists <schema_name>.<user_model>_braze_output(
  external_id string,
  payload string,
  updated_at timestamp
);

-- COMMAND ----------

-- insert into the string diff table
insert into <schema_name>.<users_model>_diff
  with users_exploded as (
    select
      external_id as id
      , explode(from_json(to_json(struct(*), map('ignoreNullFields', 'false')), 'map<string, string>'))
    from <schema_name>.<users_model>
  )
  , checkpoint_exploded as (
    select
      external_id as id
      , explode(from_json(to_json(struct(*), map('ignoreNullFields', 'false')), 'map<string, string>'))
    from <schema_name>.<users_model>_diff_checkpoint
  )
  select
      coalesce(u.id, t.id) as user_id
      , coalesce(u.key, t.key) as key
      , t.value as old_value
      , u.value as new_value
      , current_timestamp() as created_ts
  from users_exploded u
  full outer join checkpoint_exploded t on u.id = t.id and u.key = t.key
  where not (u.value <=> t.value);


-- COMMAND ----------

-- insert into the granular diff table
insert into <schema_name>.<users_model>_granular_diff
  with type_data as (
    select '[
      REPLACE EXAMPLE VALUES BELOW
      {
        "col_name": "Watchlist Item IDs",
        "datatype": "array"
      },
      {
        "col_name": "Last Purchased Item",
        "datatype": "json"
      },
      {
        "col_name": "Items Purchased",
        "datatype": "json_array",
        -- you must specify the ID key for Braze to use when updated items
        "id_key": "transaction_id"
      }
    ]' as data
  )
  , raw as (
    select
      explode(from_json(data, 'array<map<string, string>>')) as row
    from type_data
  )
  , type_map as (
    select 
      row.col_name
      , row.datatype
      , row.id_key
    from raw
  )
  , diff as (
    select * from <schema_name>.<users_model>_diff d
    left join type_map tm on  d.key = tm.col_name
    where created_ts > (
      select coalesce(max(created_ts), to_timestamp('1970-01-01', 'yyyy-MM-dd'))
      from <schema_name>.<users_model>_granular_diff
    )
  )
  , regular_diff as (
    select 
      user_id
      , key
      , old_value
      , new_value
      , new_value as diff
      , datatype
    from diff d
    where isnull(datatype)
  )  
  , array_diff as (
    select 
      user_id
      , key
      , old_value
      , new_value
      , array_diff(
            coalesce(old_value, "[]"), 
            coalesce(new_value, "[]")
      ) as diff
      , datatype
    from diff d
    where datatype = "array"
  )
  , json_diff as (
    select 
      user_id
      , key
      , old_value
      , new_value
      , json_deep_diff(
            coalesce(old_value, "{}"), 
            coalesce(new_value, "{}")
      ) as diff
      , datatype
    from diff d
    where datatype = "json"
  )
  , json_array_diff as (
    select 
      user_id
      , key
      , old_value
      , new_value
      , replace(
          json_array_deep_diff(
            coalesce(old_value, "[]"), 
            coalesce(new_value, "[]"),
            id_key
          ), 
          '__dollar__', 
          '$'
        ) as diff
      , datatype
    from diff d
    where datatype = "json_array"
  )
  , combined as (
    select * from regular_diff 
    union all
    select * from array_diff 
    union all
    select * from json_diff
    union all
    select * from json_array_diff
  )

  select
    c.user_id
    , c.key
    , c.old_value
    , c.new_value
    , c.diff
    , c.datatype
    , current_timestamp() as created_ts 
  from combined c
  order by user_id asc


-- COMMAND ----------

-- format into Braze output. Group by user_id
insert into <schema_name>.<users_model>_braze_output
  select 
  user_id as external_id
  , json_from_items(to_json(collect_list(struct(
      key,
      replace(diff, '"', '__quote__') as value,
      datatype
    ))), '{"_merge_objects": true}') as payload
  , current_timestamp() as updated_at
  from <schema_name>.<users_model>_granular_diff
  where key != 'external_id'
  and created_ts > (
    select coalesce(max(updated_at), to_timestamp('1970-01-01', 'yyyy-MM-dd'))
    from <schema_name>.<users_model>_braze_output
  )
  group by user_id


