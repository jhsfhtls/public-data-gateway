{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(ano_mes_competencia as varchar) as ano_mes_competencia,
    cast(cnpj_14 as varchar) as cnpj_14,
    cast(laboratorio_detentor_referencia as varchar) as laboratorio_detentor_referencia,
    cast(total_apresentacoes_ativas_cmed as integer) as total_apresentacoes_ativas_cmed,
    cast(apresentacoes_genericos as integer) as apresentacoes_genericos,
    cast(apresentacoes_similares as integer) as apresentacoes_similares,
    cast(apresentacoes_referencia_novos_biologicos as integer) as apresentacoes_referencia_novos_biologicos,
    cast(participacao_genericos_similares_pct as double) as participacao_genericos_similares_pct,
    cast(apresentacoes_tarja_vermelha_preta_pct as double) as apresentacoes_tarja_vermelha_preta_pct,
    cast(apresentacoes_hospitalares_restritas_pct as double) as apresentacoes_hospitalares_restritas_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('anvisa_cmed_silver', 'cmed_apresentacoes_registradas_municipio') }}
