{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(total_beneficiarios_med_odo as bigint) as total_beneficiarios_med_odo,
    cast(taxa_cobertura_privada_pct as double) as taxa_cobertura_privada_pct,
    cast(idss_medio_ponderado as double) as idss_medio_ponderado,
    cast(sinistralidade_assistencial_pct as double) as sinistralidade_assistencial_pct,
    cast(receita_contraprestacoes_mil_brl as double) as receita_contraprestacoes_mil_brl
from {{ source('ans_saude_suplementar', 'ans_saude_suplementar_resumo_municipal') }}
