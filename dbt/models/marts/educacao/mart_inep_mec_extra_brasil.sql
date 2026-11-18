{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        proficiencia_saeb_matematica,
        proficiencia_saeb_linguagens,
        beneficiarios_prouni_sisu_total,
        repasses_fundeb_mil_brl,
        bolsas_cnpq_100k_hab,
        qtd_etapas_avaliadas_inep,
        media_geral_saeb_lp_mat,
        bolsas_prouni_ativas,
        vagas_ofertadas_sisu,
        nota_corte_media_sisu,
        taxa_ocupacao_vagas_pct,
        investimento_aluno_ano_fundeb_brl,
        bolsas_pesquisa_cnpq_ativas,
        projetos_ct_cnpq_vigentes,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.45 * least(100.0, (media_geral_saeb_lp_mat / 310.0) * 100.0)
                    + 0.30 * taxa_ocupacao_vagas_pct
                    + 0.25 * least(100.0, (ln(1.0 + bolsas_pesquisa_cnpq_ativas) / 10.0) * 100.0)
                )
            ),
            2
        ) as score_capital_humano_mec_extra
    from {{ ref('fct_inep_mec_extra_municipal') }}
)

select
    *,
    case
        when score_capital_humano_mec_extra >= 78.0
            then 'POLO_EXCELENCIA_EDUCACIONAL_MEC'
        when score_capital_humano_mec_extra >= 60.0
            then 'ECOSSISTEMA_ENSINO_CONSOLIDADO'
        else 'EXPANSAO_ACESSO_EDUCACIONAL'
    end as tier_ecossistema_educacional_mec
from scored
