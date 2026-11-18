{{ config(materialized='view') }}

select
    cast(cod_operacao_bndes as varchar) as cod_operacao_bndes,
    cast(modalidade_apoio_bndes as varchar) as modalidade_apoio_bndes,
    cast(setor_cnae_fomento as varchar) as setor_cnae_fomento,
    cast(instrumento_financeiro as varchar) as instrumento_financeiro,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(qtd_operacoes_contratadas as bigint) as qtd_operacoes_contratadas,
    cast(valor_contratado_bndes_mil_brl as double) as valor_contratado_bndes_mil_brl,
    cast(prazo_medio_amortizacao_meses as integer) as prazo_medio_amortizacao_meses
from {{ source('bndes_operacoes', 'bndes_operacoes_credito_municipio') }}
