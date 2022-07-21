{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_o2obots_work",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('o2obots_work', '_airbyte_raw_profiles') }}
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as id,
    {{ json_extract_scalar('_airbyte_data', ['cpf'], ['cpf']) }} as cpf,
    {{ json_extract_scalar('_airbyte_data', ['LGPD'], ['LGPD']) }} as lgpd,
    {{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as {{ adapter.quote('name') }},
    {{ json_extract_scalar('_airbyte_data', ['email'], ['email']) }} as email,
    {{ json_extract_scalar('_airbyte_data', ['phone'], ['phone']) }} as phone,
    {{ json_extract_scalar('_airbyte_data', ['extras'], ['extras']) }} as extras,
    {{ json_extract_scalar('_airbyte_data', ['income'], ['income']) }} as income,
    {{ json_extract_scalar('_airbyte_data', ['zipcode'], ['zipcode']) }} as zipcode,
    {{ json_extract_scalar('_airbyte_data', ['birthday'], ['birthday']) }} as birthday,
    {{ json_extract_scalar('_airbyte_data', ['broker_id'], ['broker_id']) }} as broker_id,
    {{ json_extract_scalar('_airbyte_data', ['last_seen'], ['last_seen']) }} as last_seen,
    {{ json_extract_scalar('_airbyte_data', ['company_id'], ['company_id']) }} as company_id,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as created_at,
    {{ json_extract_scalar('_airbyte_data', ['hubspot_id'], ['hubspot_id']) }} as hubspot_id,
    {{ json_extract_scalar('_airbyte_data', ['last_robot'], ['last_robot']) }} as last_robot,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as updated_at,
    {{ json_extract_scalar('_airbyte_data', ['messenger_id'], ['messenger_id']) }} as messenger_id,
    {{ json_extract_scalar('_airbyte_data', ['ourhubspot_id'], ['ourhubspot_id']) }} as ourhubspot_id,
    {{ json_extract_scalar('_airbyte_data', ['marital_status'], ['marital_status']) }} as marital_status,
    {{ json_extract_scalar('_airbyte_data', ['professional_situation'], ['professional_situation']) }} as professional_situation,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('o2obots_work', '_airbyte_raw_profiles') }} as table_alias
-- profiles
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

