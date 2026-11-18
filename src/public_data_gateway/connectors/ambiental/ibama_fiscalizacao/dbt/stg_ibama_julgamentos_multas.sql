{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/ibama_fiscalizacao/silver/ibama_julgamentos_multas.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(qtd_processos_julgados as bigint) as qtd_processos_julgados,
    cast(valor_multas_arbitradas_brl as double) as valor_multas_arbitradas_brl,
    cast(valor_multas_arrecadadas_brl as double) as valor_multas_arrecadadas_brl,
    cast(percentual_divida_inscrita_pgfn_pct as double) as percentual_divida_inscrita_pgfn_pct,
    cast(indice_recuperacao_embargos_pct as double) as indice_recuperacao_embargos_pct
from source
