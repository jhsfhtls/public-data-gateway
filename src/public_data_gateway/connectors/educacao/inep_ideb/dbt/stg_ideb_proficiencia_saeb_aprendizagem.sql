{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(cep_8 as varchar) as cep_8,
    cast(ano_edicao_ideb as bigint) as ano_edicao_ideb,
    cast(proficiencia_saeb_lp_5ano as double) as proficiencia_saeb_lp_5ano,
    cast(proficiencia_saeb_mat_5ano as double) as proficiencia_saeb_mat_5ano,
    cast(proficiencia_saeb_lp_9ano as double) as proficiencia_saeb_lp_9ano,
    cast(proficiencia_saeb_mat_9ano as double) as proficiencia_saeb_mat_9ano,
    cast(aprendizado_adequado_anos_iniciais_pct as double) as aprendizado_adequado_anos_iniciais_pct,
    cast(aprendizado_adequado_anos_finais_pct as double) as aprendizado_adequado_anos_finais_pct,
    cast(taxa_participacao_saeb_pct as double) as taxa_participacao_saeb_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('inep_ideb_silver', 'ideb_proficiencia_saeb_aprendizagem') }}
