{{ config(materialized='view') }}

select
    cast(codigo_evento as varchar) as codigo_evento,
    cast(regime_afetado as varchar) as regime_afetado,
    cast(tipo_evento as varchar) as tipo_evento,
    cast(descricao_evento as varchar) as descricao_evento,
    cast(motivo_legal as varchar) as motivo_legal
from {{ source('rfb_simples_mei', 'simples_eventos_historico') }}
