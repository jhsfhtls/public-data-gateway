{{ config(materialized='view') }}

select
    cast(cod_folha_siape as varchar) as cod_folha_siape,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(remuneracao_media_bruta_brl as double) as remuneracao_media_bruta_brl,
    cast(remuneracao_teto_pct as double) as remuneracao_teto_pct,
    cast(adicional_funcao_das_fce_mil_brl as double) as adicional_funcao_das_fce_mil_brl,
    cast(competencia_siape as varchar) as competencia_siape
from {{ source('cgu_servidores_federais', 'siape_remuneracao_folha_orgao') }}
where id_municipio is not null
