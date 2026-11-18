{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(pescadores_seguro_defeso_ativos as bigint) as pescadores_seguro_defeso_ativos,
    cast(agricultores_garantia_safra as bigint) as agricultores_garantia_safra,
    cast(repasses_rurais_emergenciais_mil_brl as double) as repasses_rurais_emergenciais_mil_brl,
    cast(alertas_elegibilidade_cgu_pct as double) as alertas_elegibilidade_cgu_pct
from {{ source('cgu_beneficios_cidadao', 'cgu_seguro_defeso_safra') }}
