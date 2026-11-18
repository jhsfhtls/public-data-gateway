{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(servidores_dedicacao_exclusiva_pct as double) as servidores_dedicacao_exclusiva_pct,
    cast(alertas_acumulo_ilicito as integer) as alertas_acumulo_ilicito,
    cast(servidores_cedidos_count as integer) as servidores_cedidos_count,
    cast(indice_integridade_siape_pct as double) as indice_integridade_siape_pct
from {{ source('cgu_servidores_federais', 'cgu_acumulo_cargos_conflitos') }}
where id_municipio is not null
