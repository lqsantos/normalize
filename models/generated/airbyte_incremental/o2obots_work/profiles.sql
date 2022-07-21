{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "o2obots_work",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('profiles_ab3') }}
select
    id,
    cpf,
    lgpd,
    {{ adapter.quote('name') }},
    email,
    phone,
    extras,
    income,
    zipcode,
    birthday,
    broker_id,
    last_seen,
    company_id,
    created_at,
    hubspot_id,
    last_robot,
    updated_at,
    messenger_id,
    ourhubspot_id,
    marital_status,
    professional_situation,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_profiles_hashid
from {{ ref('profiles_ab3') }}
-- profiles from {{ source('o2obots_work', '_airbyte_raw_profiles') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

