{{ config(materialized='view') }}

select
    cast(cod_concessao_inss as varchar) as cod_concessao_inss,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(qtd_concessoes_ano as integer) as qtd_concessoes_ano,
    cast(valor_medio_concedido_brl as double) as valor_medio_concedido_brl,
    cast(taxa_indeferimento_pct as double) as taxa_indeferimento_pct,
    cast(tempo_medio_concessao_dias as integer) as tempo_medio_concessao_dias
from {{ source('mps_inss_beneficios', 'inss_beneficios_concedidos_concessoes') }}
where id_municipio is not null
