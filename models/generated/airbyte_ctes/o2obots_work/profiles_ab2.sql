{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_o2obots_work",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('profiles_ab1') }}
select
    cast(id as {{ dbt_utils.type_float() }}) as id,
    cast(cpf as {{ dbt_utils.type_string() }}(1024)) as cpf,
    {{ cast_to_boolean('lgpd') }} as lgpd,
    cast({{ adapter.quote('name') }} as {{ dbt_utils.type_string() }}(1024)) as {{ adapter.quote('name') }},
    cast(email as {{ dbt_utils.type_string() }}(1024)) as email,
    cast(phone as {{ dbt_utils.type_string() }}(1024)) as phone,
    cast(extras as {{ dbt_utils.type_string() }}(1024)) as extras,
    cast(income as {{ dbt_utils.type_float() }}) as income,
    cast(zipcode as {{ dbt_utils.type_string() }}(1024)) as zipcode,
    cast(birthday as {{ dbt_utils.type_string() }}(1024)) as birthday,
    cast(broker_id as {{ dbt_utils.type_float() }}) as broker_id,
    cast(last_seen as {{ dbt_utils.type_string() }}(1024)) as last_seen,
    cast(company_id as {{ dbt_utils.type_float() }}) as company_id,
    cast(created_at as {{ dbt_utils.type_string() }}(1024)) as created_at,
    cast(hubspot_id as {{ dbt_utils.type_string() }}(1024)) as hubspot_id,
    cast(last_robot as {{ dbt_utils.type_string() }}(1024)) as last_robot,
    cast(updated_at as {{ dbt_utils.type_string() }}(1024)) as updated_at,
    cast(messenger_id as {{ dbt_utils.type_string() }}(1024)) as messenger_id,
    cast(ourhubspot_id as {{ dbt_utils.type_string() }}(1024)) as ourhubspot_id,
    cast(marital_status as {{ dbt_utils.type_string() }}(1024)) as marital_status,
    cast(professional_situation as {{ dbt_utils.type_string() }}(1024)) as professional_situation,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('profiles_ab1') }}
-- profiles
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

