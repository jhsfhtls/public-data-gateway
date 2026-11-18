{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(distribuidora_principal as varchar) as distribuidora_principal,
    cast(prazo_medio_vistoria_dias as double) as prazo_medio_vistoria_dias,
    cast(taxa_aprovacao_parecer_pct as double) as taxa_aprovacao_parecer_pct,
    cast(inversao_fluxo_critica_pct as double) as inversao_fluxo_critica_pct,
    cast(ucs_beneficiarias_total as integer) as ucs_beneficiarias_total
from {{ source('aneel_geracao_distribuida', 'aneel_distribuidoras_rede_mmgd') }}
where id_municipio is not null
