{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/bcb_sicor/silver/sicor_investimento_municipio_produto.parquet')
)

select
    cast(cod_investimento_sicor as varchar) as cod_investimento_sicor,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(ano_emissao as bigint) as ano_emissao,
    cast(nome_produto as varchar) as nome_produto,
    cast(cd_programa as varchar) as cd_programa,
    cast(nome_programa as varchar) as nome_programa,
    cast(qtd_contratos_investimento as bigint) as qtd_contratos_investimento,
    cast(valor_investimento_brl as double) as valor_investimento_brl
from source
