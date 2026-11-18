{{ config(materialized='table') }}

with emi as (
    select
        id_municipio,
        count(*) as qtd_grupos_especie_inss
    from {{ ref('stg_inss_beneficios_emitidos_especie') }}
    group by id_municipio
),

conc as (
    select
        id_municipio,
        qtd_concessoes_ano,
        valor_medio_concedido_brl,
        taxa_indeferimento_pct,
        tempo_medio_concessao_dias
    from {{ ref('stg_inss_beneficios_concedidos_concessoes') }}
),

arr as (
    select
        id_municipio,
        massa_contribuintes_ativos,
        arrecadacao_previdenciaria_mil_brl,
        razao_contribuintes_beneficiarios
    from {{ ref('stg_mps_arrecadacao_previdenciaria_gps') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.beneficios_inss_emitidos,
    r.valor_mensal_beneficios_mil_brl,
    r.ticket_medio_beneficio_brl,
    r.participacao_bpc_loas_pct,
    coalesce(e.qtd_grupos_especie_inss, 0) as qtd_grupos_especie_inss,
    coalesce(c.qtd_concessoes_ano, 0) as qtd_concessoes_ano,
    coalesce(c.valor_medio_concedido_brl, 0.0) as valor_medio_concedido_brl,
    coalesce(c.taxa_indeferimento_pct, 30.0) as taxa_indeferimento_pct,
    coalesce(c.tempo_medio_concessao_dias, 45) as tempo_medio_concessao_dias,
    coalesce(a.massa_contribuintes_ativos, 0) as massa_contribuintes_ativos,
    coalesce(a.arrecadacao_previdenciaria_mil_brl, 0.0) as arrecadacao_previdenciaria_mil_brl,
    coalesce(a.razao_contribuintes_beneficiarios, 1.5) as razao_contribuintes_beneficiarios
from {{ ref('stg_inss_previdencia_resumo_municipal') }} r
left join emi e on r.id_municipio = e.id_municipio
left join conc c on r.id_municipio = c.id_municipio
left join arr a on r.id_municipio = a.id_municipio
