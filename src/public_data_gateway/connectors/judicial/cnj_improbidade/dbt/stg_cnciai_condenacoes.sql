{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/cnj_improbidade/silver/cnciai_condenacoes.parquet')
)

select
    cast(id_condenacao as varchar) as id_condenacao,
    cast(numero_processo_cnj as varchar) as numero_processo_cnj,
    cast(documento_requerido as varchar) as documento_requerido,
    cast(tipo_pessoa as varchar) as tipo_pessoa,
    cast(nome_requerido as varchar) as nome_requerido,
    cast(esfera_cargo as varchar) as esfera_cargo,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(tribunal_origem as varchar) as tribunal_origem,
    cast(grau_jurisdicao as varchar) as grau_jurisdicao,
    cast(proibicao_contratar_poder_publico as boolean) as proibicao_contratar_poder_publico,
    cast(suspensao_direitos_politicos as boolean) as suspensao_direitos_politicos,
    cast(valor_ressarcimento_multa_brl as double) as valor_ressarcimento_multa_brl,
    cast(data_transito_julgado as varchar) as data_transito_julgado
from source
