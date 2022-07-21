{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_o2obots_work",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('profiles_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'id',
        'cpf',
        boolean_to_string('lgpd'),
        adapter.quote('name'),
        'email',
        'phone',
        'extras',
        'income',
        'zipcode',
        'birthday',
        'broker_id',
        'last_seen',
        'company_id',
        'created_at',
        'hubspot_id',
        'last_robot',
        'updated_at',
        'messenger_id',
        'ourhubspot_id',
        'marital_status',
        'professional_situation',
    ]) }} as _airbyte_profiles_hashid,
    tmp.*
from {{ ref('profiles_ab2') }} tmp
-- profiles
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

