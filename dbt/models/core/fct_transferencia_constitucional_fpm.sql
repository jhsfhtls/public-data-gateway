{{ config(materialized='table') }}

with fpm as (
    select * from {{ ref('stg_stn_transferencias_fpm') }}
)

select
    id_municipio_ibge_7d,
    sigla_uf,
    nome_municipio,
    ano_exercicio,
    mes_competencia,
    sum(valor_bruto_fpm) as valor_bruto_fpm_mes,
    sum(deducao_fundeb) as deducao_fundeb_mes,
    sum(deducao_saude) as deducao_saude_mes,
    sum(valor_liquido_fpm) as valor_liquido_fpm_mes,
    count(distinct decendio) as total_decendios_creditados
from fpm
group by
    id_municipio_ibge_7d,
    sigla_uf,
    nome_municipio,
    ano_exercicio,
    mes_competencia
