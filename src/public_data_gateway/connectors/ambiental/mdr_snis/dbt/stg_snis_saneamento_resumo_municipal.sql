{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(prestador_servico as varchar) as prestador_servico,
    cast(atendimento_agua_total_pct as double) as atendimento_agua_total_pct,
    cast(atendimento_esgoto_total_pct as double) as atendimento_esgoto_total_pct,
    cast(esgoto_tratado_sobre_consumido_pct as double) as esgoto_tratado_sobre_consumido_pct,
    cast(indice_perdas_distribuicao_pct as double) as indice_perdas_distribuicao_pct,
    cast(cobertura_coleta_rdo_pct as double) as cobertura_coleta_rdo_pct,
    cast(investimento_total_milhoes_brl as double) as investimento_total_milhoes_brl
from {{ source('mdr_snis', 'snis_saneamento_resumo_municipal') }}
where id_municipio is not null
