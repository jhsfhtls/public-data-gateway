{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(qtd_convenios_ativos as integer) as qtd_convenios_ativos,
    cast(emendas_parlamentares_pagas_mil_brl as double) as emendas_parlamentares_pagas_mil_brl,
    cast(adimplencia_prestacao_contas_pct as double) as adimplencia_prestacao_contas_pct,
    cast(uso_cpgf_cartao_mil_brl as double) as uso_cpgf_cartao_mil_brl
from {{ source('cgu_transparencia_gastos', 'cgu_emendas_convenios_municipio') }}
where id_municipio is not null
