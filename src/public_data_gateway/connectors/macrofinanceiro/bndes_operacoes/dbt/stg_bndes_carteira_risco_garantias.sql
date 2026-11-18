{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(juros_medio_tjlp_tlp_aa_pct as double) as juros_medio_tjlp_tlp_aa_pct,
    cast(inadimplencia_carteira_bndes_pct as double) as inadimplencia_carteira_bndes_pct,
    cast(cobertura_fgi_garantias_pct as double) as cobertura_fgi_garantias_pct,
    cast(projetos_sustentaveis_count as bigint) as projetos_sustentaveis_count
from {{ source('bndes_operacoes', 'bndes_carteira_risco_garantias') }}
