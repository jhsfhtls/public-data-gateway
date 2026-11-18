{{ config(materialized='view') }}

select
    cast(cod_coleta_anp as varchar) as cod_coleta_anp,
    cast(produto_combustivel as varchar) as produto_combustivel,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(preco_medio_revenda_brl as double) as preco_medio_revenda_brl,
    cast(preco_medio_distribuicao_brl as double) as preco_medio_distribuicao_brl,
    cast(margem_media_revenda_brl as double) as margem_media_revenda_brl,
    cast(desvio_padrao_revenda_brl as double) as desvio_padrao_revenda_brl,
    cast(qtd_postos_pesquisados as integer) as qtd_postos_pesquisados,
    cast(data_coleta as varchar) as data_coleta
from {{ source('anp_combustiveis', 'anp_precos_revenda_municipio') }}
where cod_coleta_anp is not null
  and id_municipio is not null
