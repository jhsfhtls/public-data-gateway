{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/ibama_fiscalizacao/silver/ibama_resumo_municipal.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(qtd_autos_infracao_ibama as bigint) as qtd_autos_infracao_ibama,
    cast(qtd_embargos_ativos_ibama as bigint) as qtd_embargos_ativos_ibama,
    cast(area_embargada_ibama_ha as double) as area_embargada_ibama_ha,
    cast(valor_multas_ibama_brl as double) as valor_multas_ibama_brl,
    cast(taxa_infracoes_flora_pct as double) as taxa_infracoes_flora_pct
from source
