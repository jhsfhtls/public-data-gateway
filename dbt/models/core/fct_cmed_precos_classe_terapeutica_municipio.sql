{{ config(materialized='table') }}

with classes as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        ano_mes_competencia,
        cnpj_14,
        classe_terapeutica_atc,
        apresentacoes_classe,
        participacao_classe_pct,
        preco_fabrica_pf_medio_brl,
        preco_maximo_consumidor_pmc_medio_brl,
        preco_maximo_venda_governo_pmvg_medio_brl,
        desconto_medio_pmvg_sobre_pf_pct,
        reference_date,
        extracted_at
    from {{ ref('stg_cmed_precos_regulados_classe_terapeutica') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    c.id_municipio,
    c.nome_municipio,
    c.sigla_uf,
    c.id_uf,
    coalesce(m.nome_uf, c.sigla_uf) as nome_uf,
    coalesce(m.regiao, 'Brasil') as regiao,
    c.ano_mes_competencia,
    c.cnpj_14,
    c.classe_terapeutica_atc,
    c.apresentacoes_classe,
    c.participacao_classe_pct,
    c.preco_fabrica_pf_medio_brl,
    c.preco_maximo_consumidor_pmc_medio_brl,
    c.preco_maximo_venda_governo_pmvg_medio_brl,
    c.desconto_medio_pmvg_sobre_pf_pct,
    c.reference_date,
    c.extracted_at
from classes c
left join municipios m on c.id_municipio = m.id_municipio
