{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/bcb_sicor/silver/sicor_custeio_municipio_produto.parquet')
)

select
    cast(cod_operacao_sicor as varchar) as cod_operacao_sicor,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(ano_emissao as bigint) as ano_emissao,
    cast(mes_emissao as varchar) as mes_emissao,
    cast(cd_produto as varchar) as cd_produto,
    cast(nome_produto as varchar) as nome_produto,
    cast(atividade as varchar) as atividade,
    cast(cd_programa as varchar) as cd_programa,
    cast(nome_programa as varchar) as nome_programa,
    cast(qtd_contratos_custeio as bigint) as qtd_contratos_custeio,
    cast(valor_custeio_brl as double) as valor_custeio_brl,
    cast(area_financiada_ha as double) as area_financiada_ha
from source
