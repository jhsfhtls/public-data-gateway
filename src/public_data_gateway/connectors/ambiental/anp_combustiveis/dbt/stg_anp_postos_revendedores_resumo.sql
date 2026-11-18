{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(qtd_postos_ativos_anp as integer) as qtd_postos_ativos_anp,
    cast(participacao_bandeira_branca_pct as double) as participacao_bandeira_branca_pct,
    cast(taxa_conformidade_pmqc_pct as double) as taxa_conformidade_pmqc_pct,
    cast(capacidade_armazenagem_m3 as double) as capacidade_armazenagem_m3
from {{ source('anp_combustiveis', 'anp_postos_revendedores_resumo') }}
where id_municipio is not null
