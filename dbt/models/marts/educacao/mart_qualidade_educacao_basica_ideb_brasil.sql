{{ config(materialized='table') }}

with base as (
    select
        *,
        least(100.0, greatest(0.0, ((ideb_anos_iniciais_rede_publica - 4.5) / 2.5) * 100.0)) as s_ai,
        least(100.0, greatest(0.0, ((ideb_anos_finais_rede_publica - 3.8) / 2.2) * 100.0)) as s_af,
        least(100.0, greatest(0.0, ((ideb_ensino_medio_rede_publica - 3.4) / 2.0) * 100.0)) as s_em,
        least(100.0, greatest(0.0, (proporcao_escolas_cumpriram_meta_pct / 85.0) * 100.0)) as s_meta,
        least(100.0, greatest(0.0, ((indicador_rendimento_fluxo_medio_p - 0.80) / 0.18) * 100.0)) as s_fluxo,
        least(100.0, greatest(0.0, (aprendizado_adequado_anos_iniciais_pct / 75.0) * 100.0)) as s_ap_ai,
        least(100.0, greatest(0.0, (aprendizado_adequado_anos_finais_pct / 58.0) * 100.0)) as s_ap_af,
        least(100.0, greatest(0.0, ((equidade_aprendizagem_inse_ratio - 0.60) / 0.35) * 100.0)) as s_eq,
        least(100.0, greatest(0.0, ((taxa_participacao_saeb_pct - 80.0) / 18.0) * 100.0)) as s_part
    from {{ ref('fct_ideb_desempenho_aprendizagem_municipio') }}
),

scored as (
    select
        *,
        round(
            0.30 * s_ai
            + 0.25 * s_af
            + 0.20 * s_em
            + 0.15 * s_meta
            + 0.10 * s_fluxo,
            2
        ) as score_desempenho_ideb_municipal,
        round(
            0.35 * s_ap_ai
            + 0.30 * s_ap_af
            + 0.20 * s_eq
            + 0.15 * s_part,
            2
        ) as score_proficiencia_equidade_saeb
    from base
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    regiao,
    cep_8,
    ano_edicao_ideb,
    ideb_anos_iniciais_rede_publica,
    ideb_anos_finais_rede_publica,
    ideb_ensino_medio_rede_publica,
    ideb_rede_privada_ensino_medio,
    indicador_rendimento_fluxo_medio_p,
    nota_padronizada_saeb_n_media,
    escolas_avaliadas_ideb_municipio,
    proporcao_escolas_cumpriram_meta_pct,
    proficiencia_saeb_lp_5ano,
    proficiencia_saeb_mat_5ano,
    proficiencia_saeb_lp_9ano,
    proficiencia_saeb_mat_9ano,
    aprendizado_adequado_anos_iniciais_pct,
    aprendizado_adequado_anos_finais_pct,
    taxa_participacao_saeb_pct,
    ideb_edicao_anterior_2021,
    variacao_bienal_ideb_pontos,
    gap_meta_anos_iniciais_pontos,
    gap_meta_anos_finais_pontos,
    gap_meta_ensino_medio_pontos,
    gap_rede_privada_vs_publica_em,
    equidade_aprendizagem_inse_ratio,
    score_desempenho_ideb_municipal,
    score_proficiencia_equidade_saeb,
    case
        when score_desempenho_ideb_municipal >= 78.0 and score_proficiencia_equidade_saeb >= 76.0
            then 'REDE_EXCELENCIA_APRENDIZAGEM_IDEB'
        when score_desempenho_ideb_municipal >= 62.0
            then 'DESEMPENHO_EDUCACIONAL_CONSOLIDADO_ACIMA_META'
        when score_desempenho_ideb_municipal >= 46.0
            then 'TRANSICAO_EDUCACIONAL_RECUPERACAO_FLUXO'
        else 'TERRITORIO_PRIORITARIO_RECOMPOSICAO_APRENDIZAGEM'
    end as classificacao_qualidade_ideb_municipio,
    reference_date,
    extracted_at
from scored
