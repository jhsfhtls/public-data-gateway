{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/ibge_agro_municipal/silver/ibge_pam_lavouras.parquet')
)

select
    cast(cod_registro_pam as varchar) as cod_registro_pam,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(ano_referencia as bigint) as ano_referencia,
    cast(cod_cultura_sidra as varchar) as cod_cultura_sidra,
    cast(nome_cultura as varchar) as nome_cultura,
    cast(tipo_lavoura as varchar) as tipo_lavoura,
    cast(area_plantada_ha as double) as area_plantada_ha,
    cast(area_colhida_ha as double) as area_colhida_ha,
    cast(quantidade_produzida_t as double) as quantidade_produzida_t,
    cast(valor_producao_mil_brl as double) as valor_producao_mil_brl
from source
