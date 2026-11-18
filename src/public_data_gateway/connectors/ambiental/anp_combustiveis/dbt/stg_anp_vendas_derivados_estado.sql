{{ config(materialized='view') }}

select
    cast(cod_venda_uf_anp as varchar) as cod_venda_uf_anp,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(categoria_combustivel as varchar) as categoria_combustivel,
    cast(volume_vendido_m3 as double) as volume_vendido_m3,
    cast(participacao_biocombustivel_pct as double) as participacao_biocombustivel_pct,
    cast(ano_referencia as integer) as ano_referencia
from {{ source('anp_combustiveis', 'anp_vendas_derivados_estado') }}
where cod_venda_uf_anp is not null
  and id_municipio is not null
