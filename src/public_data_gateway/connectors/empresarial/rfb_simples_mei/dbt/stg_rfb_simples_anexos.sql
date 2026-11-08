{{ config(materialized='view') }}

select
    cast(codigo_anexo as varchar) as codigo_anexo,
    cast(descricao_anexo as varchar) as descricao_anexo,
    cast(faixa_faturamento as integer) as faixa_faturamento,
    cast(receita_bruta_acumulada_min as double) as receita_bruta_acumulada_min,
    cast(receita_bruta_acumulada_max as double) as receita_bruta_acumulada_max,
    cast(aliquota_nominal_pct as double) as aliquota_nominal_pct,
    cast(parcela_a_deduzir_reais as double) as parcela_a_deduzir_reais
from {{ source('rfb_simples_mei', 'simples_tabelas_anexos') }}
