{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_edicao_enem as bigint) as ano_edicao_enem,
    cast(inscritos_confirmados_enem_ano as bigint) as inscritos_confirmados_enem_ano,
    cast(participantes_presentes_2dias_ano as bigint) as participantes_presentes_2dias_ano,
    cast(taxa_presenca_efetiva_2dias_pct as double) as taxa_presenca_efetiva_2dias_pct,
    cast(nota_media_linguagens_codigos_lc as double) as nota_media_linguagens_codigos_lc,
    cast(nota_media_ciencias_humanas_ch as double) as nota_media_ciencias_humanas_ch,
    cast(nota_media_ciencias_natureza_cn as double) as nota_media_ciencias_natureza_cn,
    cast(nota_media_matematica_mt as double) as nota_media_matematica_mt,
    cast(nota_media_objetivas_tri_geral as double) as nota_media_objetivas_tri_geral,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('inep_enem_silver', 'enem_desempenho_municipal_areas') }}
