{% snapshot users_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='user_id',

      strategy='check',
      check_cols=['first_name', 'last_name', 'address_id', 'phone_number']

    )
}}

SELECT * FROM {{ ref('stg_users_ej1') }}
-- Devuelve las filas insertadas o modificadas y las filas originales sin modificar
WHERE f_carga = (SELECT max(f_carga) FROM {{ ref('stg_users_ej1') }})

{% endsnapshot %}