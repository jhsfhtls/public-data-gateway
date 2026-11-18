{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/bcb_sicor/silver/sicor_pronaf_proagro_cobertura.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(participacao_pronaf_pct as double) as participacao_pronaf_pct,
    cast(cobertura_proagro_sicor_pct as double) as cobertura_proagro_sicor_pct,
    cast(participacao_abc_renovagro_pct as double) as participacao_abc_renovagro_pct,
    cast(taxa_glosa_res_bcb_140_pct as double) as taxa_glosa_res_bcb_140_pct
from source
