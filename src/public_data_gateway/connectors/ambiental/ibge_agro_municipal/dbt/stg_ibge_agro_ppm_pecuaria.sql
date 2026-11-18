{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/ibge_agro_municipal/silver/ibge_ppm_pecuaria.parquet')
)

select
    cast(cod_registro_ppm as varchar) as cod_registro_ppm,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(ano_referencia as bigint) as ano_referencia,
    cast(especie_rebanho as varchar) as especie_rebanho,
    cast(efetivo_rebanho_cabecas as bigint) as efetivo_rebanho_cabecas,
    cast(producao_leite_mil_litros as double) as producao_leite_mil_litros,
    cast(valor_producao_pecuaria_mil_brl as double) as valor_producao_pecuaria_mil_brl
from source
