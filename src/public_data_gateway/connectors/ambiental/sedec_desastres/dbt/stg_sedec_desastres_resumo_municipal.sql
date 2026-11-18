{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(qtd_desastres_s2id_5a as integer) as qtd_desastres_s2id_5a,
    cast(qtd_decretos_emergencia_5a as integer) as qtd_decretos_emergencia_5a,
    cast(total_afetados_5a as integer) as total_afetados_5a,
    cast(prejuizos_economicos_mil_brl as double) as prejuizos_economicos_mil_brl,
    cast(alertas_cemaden_12m as integer) as alertas_cemaden_12m
from {{ source('sedec_desastres', 'sedec_desastres_resumo_municipal') }}
where id_municipio is not null
