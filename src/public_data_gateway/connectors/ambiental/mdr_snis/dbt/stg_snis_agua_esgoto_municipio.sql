{{ config(materialized='view') }}

select
    cast(cod_snis_ae as varchar) as cod_snis_ae,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(prestador_servico as varchar) as prestador_servico,
    cast(natureza_juridica_prestador as varchar) as natureza_juridica_prestador,
    cast(atendimento_agua_total_pct as double) as atendimento_agua_total_pct,
    cast(atendimento_esgoto_total_pct as double) as atendimento_esgoto_total_pct,
    cast(esgoto_tratado_sobre_consumido_pct as double) as esgoto_tratado_sobre_consumido_pct,
    cast(indice_perdas_distribuicao_pct as double) as indice_perdas_distribuicao_pct,
    cast(ano_referencia as integer) as ano_referencia
from {{ source('mdr_snis', 'snis_agua_esgoto_municipio') }}
where cod_snis_ae is not null
  and id_municipio is not null
