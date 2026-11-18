{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(qtd_pluviometros_cemaden as integer) as qtd_pluviometros_cemaden,
    cast(populacao_area_risco_pct as double) as populacao_area_risco_pct,
    cast(possui_plano_contingencia_ativo as boolean) as possui_plano_contingencia_ativo,
    cast(alertas_risco_alto_12m as integer) as alertas_risco_alto_12m
from {{ source('sedec_desastres', 'sedec_cemaden_monitoramento') }}
where id_municipio is not null
