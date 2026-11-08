{{ config(materialized='view') }}

with source as (
    select * from {{ source('pncp_contratacoes', 'pncp_modalidades') }}
),

renamed as (
    select
        cast(modalidade_id as integer) as modalidade_id,
        trim(modalidade_nome) as modalidade_nome,
        trim(amparo_legal) as amparo_legal,
        trim(descricao) as descricao,
        cast(exige_disputa as boolean) as is_exige_disputa
    from source
)

select * from renamed
