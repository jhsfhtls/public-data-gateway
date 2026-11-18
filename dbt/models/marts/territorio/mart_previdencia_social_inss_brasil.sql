{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        beneficios_inss_emitidos,
        valor_mensal_beneficios_mil_brl,
        ticket_medio_beneficio_brl,
        participacao_bpc_loas_pct,
        qtd_grupos_especie_inss,
        qtd_concessoes_ano,
        valor_medio_concedido_brl,
        taxa_indeferimento_pct,
        tempo_medio_concessao_dias,
        massa_contribuintes_ativos,
        arrecadacao_previdenciaria_mil_brl,
        razao_contribuintes_beneficiarios,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    least(45.0, razao_contribuintes_beneficiarios * 20.0)
                    + greatest(0.0, 40.0 - participacao_bpc_loas_pct * 1.1)
                    + least(15.0, ticket_medio_beneficio_brl / 180.0)
                )
            ),
            2
        ) as score_cobertura_previdenciaria_inss
    from {{ ref('fct_previdencia_social_inss_municipal') }}
)

select
    *,
    case
        when score_cobertura_previdenciaria_inss >= 75.0
            then 'PREVIDENCIA_SUPERAVITARIA_FORMAL'
        when score_cobertura_previdenciaria_inss >= 50.0
            then 'COBERTURA_PREVIDENCIARIA_ESTAVEL'
        else 'ALTA_DEPENDENCIA_ASSISTENCIAL_BPC'
    end as tier_sustentabilidade_previdenciaria
from scored
