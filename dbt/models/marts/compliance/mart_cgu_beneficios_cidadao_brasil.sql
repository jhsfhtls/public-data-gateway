{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        familias_bolsa_familia,
        beneficiarios_bpc_loas,
        volume_mensal_transferencias_mil_brl,
        valor_medio_beneficio_brl,
        cobertura_cadunico_pobreza_pct,
        qtd_programas_sociais_cgu,
        soma_transferencias_bf_bpc_mil_brl,
        valor_mensal_bpc_mil_brl,
        parcelas_auxilio_gas_ativas,
        pescadores_seguro_defeso_ativos,
        agricultores_garantia_safra,
        repasses_rurais_emergenciais_mil_brl,
        alertas_elegibilidade_cgu_pct,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.50 * cobertura_cadunico_pobreza_pct
                    + 0.30 * greatest(0.0, 100.0 - alertas_elegibilidade_cgu_pct * 5.0)
                    + 0.20 * least(100.0, (ln(1.0 + volume_mensal_transferencias_mil_brl) / 14.0) * 100.0)
                )
            ),
            2
        ) as score_eficacia_protecao_social_cgu
    from {{ ref('fct_cgu_beneficios_cidadao_municipal') }}
)

select
    *,
    case
        when score_eficacia_protecao_social_cgu >= 78.0
            then 'REDE_PROTECAO_SOCIAL_AMPLA_AUDITADA'
        when score_eficacia_protecao_social_cgu >= 60.0
            then 'COBERTURA_ASSISTENCIAL_CONSOLIDADA'
        else 'DEMANDA_BUSCA_ATIVA_VULNERABILIDADE'
    end as tier_protecao_social_cidadao
from scored
