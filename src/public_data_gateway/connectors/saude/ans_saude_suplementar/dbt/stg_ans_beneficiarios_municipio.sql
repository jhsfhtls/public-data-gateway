{{ config(materialized='view') }}

select
    cast(cod_cobertura_ans as varchar) as cod_cobertura_ans,
    cast(segmentacao_assistencial as varchar) as segmentacao_assistencial,
    cast(contratacao_tipo as varchar) as contratacao_tipo,
    cast(situacao_registro_ans as varchar) as situacao_registro_ans,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(beneficiarios_ativos_count as bigint) as beneficiarios_ativos_count,
    cast(taxa_cobertura_privada_pct as double) as taxa_cobertura_privada_pct,
    cast(competencia_ans as varchar) as competencia_ans
from {{ source('ans_saude_suplementar', 'ans_beneficiarios_municipio') }}
