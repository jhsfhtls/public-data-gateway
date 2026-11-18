{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(total_servidores_federais_siape as integer) as total_servidores_federais_siape,
    cast(folha_mensal_bruta_siape_mil_brl as double) as folha_mensal_bruta_siape_mil_brl,
    cast(remuneracao_media_bruta_brl as double) as remuneracao_media_bruta_brl,
    cast(pct_cargos_comissionados_das as double) as pct_cargos_comissionados_das
from {{ source('cgu_servidores_federais', 'siape_servidores_resumo_municipal') }}
where id_municipio is not null
