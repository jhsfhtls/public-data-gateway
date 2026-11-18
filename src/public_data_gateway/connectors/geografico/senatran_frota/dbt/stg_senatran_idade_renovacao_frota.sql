{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(idade_media_frota_anos as double) as idade_media_frota_anos,
    cast(participacao_frota_0a5_anos_pct as double) as participacao_frota_0a5_anos_pct,
    cast(taxa_motorizacao_100hab as double) as taxa_motorizacao_100hab,
    cast(veiculos_novos_emplacados_ano as bigint) as veiculos_novos_emplacados_ano
from {{ source('senatran_frota', 'senatran_idade_renovacao_frota') }}
where id_municipio is not null
