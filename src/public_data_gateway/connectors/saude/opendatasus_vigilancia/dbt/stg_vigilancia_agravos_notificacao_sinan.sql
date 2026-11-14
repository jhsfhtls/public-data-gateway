{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_epidemiologico as bigint) as ano_epidemiologico,
    cast(grupo_agravo_sinan as varchar) as grupo_agravo_sinan,
    cast(categoria_transmissao as varchar) as categoria_transmissao,
    cast(casos_notificados_ano as bigint) as casos_notificados_ano,
    cast(casos_confirmados_ano as bigint) as casos_confirmados_ano,
    cast(taxa_incidencia_confirmada_100k_hab as double) as taxa_incidencia_confirmada_100k_hab,
    cast(proporcao_encerramento_oportuno_sinan_pct as double) as proporcao_encerramento_oportuno_sinan_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('opendatasus_vigilancia_silver', 'vigilancia_agravos_notificacao_sinan') }}
