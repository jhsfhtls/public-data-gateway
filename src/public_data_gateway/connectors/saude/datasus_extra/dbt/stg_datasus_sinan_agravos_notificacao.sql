{{ config(materialized='view') }}

select
    cast(cod_sinan_id as varchar) as cod_sinan_id,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(notificacoes_dengue_12m as bigint) as notificacoes_dengue_12m,
    cast(notificacoes_respiratorias_12m as bigint) as notificacoes_respiratorias_12m,
    cast(taxa_incidencia_100k_hab as double) as taxa_incidencia_100k_hab,
    cast(oportunidade_encerramento_pct as double) as oportunidade_encerramento_pct
from {{ source('datasus_extra', 'datasus_sinan_agravos_notificacao') }}
where cod_sinan_id is not null
