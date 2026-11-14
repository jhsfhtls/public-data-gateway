{{ config(materialized='table') }}

with base as (
    select
        *,
        least(100.0, greatest(0.0, ((nota_media_objetivas_tri_geral - 450.0) / 150.0) * 100.0)) as norm_obj,
        least(100.0, greatest(0.0, ((nota_media_redacao_enem - 500.0) / 250.0) * 100.0)) as norm_red,
        least(100.0, greatest(0.0, (proporcao_redacao_900_mais_pct / 16.0) * 100.0)) as norm_r900,
        least(100.0, greatest(0.0, razao_equidade_publica_privada_enem * 100.0)) as norm_eq
    from {{ ref('fct_enem_desempenho_redacao_equidade_municipio') }}
),

scored as (
    select
        *,
        round(
            least(
                100.0,
                greatest(0.0, 0.45 * norm_obj + 0.35 * norm_red + 0.20 * norm_r900)
            )::double,
            2
        ) as score_desempenho_cognitivo_enem,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.30 * taxa_presenca_efetiva_2dias_pct
                    + 0.35 * aptos_sisu_prouni_fies_nota_450_pct
                    + 0.35 * norm_eq
                )
            )::double,
            2
        ) as score_equidade_acesso_ensino_superior_enem
    from base
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    regiao,
    ano_edicao_enem,
    inscritos_confirmados_enem_ano,
    participantes_presentes_2dias_ano,
    taxa_presenca_efetiva_2dias_pct,
    nota_media_linguagens_codigos_lc,
    nota_media_ciencias_humanas_ch,
    nota_media_ciencias_natureza_cn,
    nota_media_matematica_mt,
    nota_media_objetivas_tri_geral,
    nota_media_redacao_enem,
    nota_media_comp1_norma_padrao,
    nota_media_comp3_argumentacao,
    nota_media_comp5_proposta_intervencao,
    redacoes_nota_900_mais_ano,
    proporcao_redacao_900_mais_pct,
    taxa_redacao_zero_ou_anulada_pct,
    proporcao_isencao_taxa_inscricao_pct,
    proporcao_concluintes_escola_publica_pct,
    acesso_internet_domiciliar_q025_pct,
    gap_nota_objetiva_privada_vs_publica_pts,
    razao_equidade_publica_privada_enem,
    aptos_sisu_prouni_fies_nota_450_pct,
    indice_paridade_socioeconomica_qse,
    score_desempenho_cognitivo_enem,
    score_equidade_acesso_ensino_superior_enem,
    case
        when score_desempenho_cognitivo_enem >= 82.0
            and score_equidade_acesso_ensino_superior_enem >= 82.0
            then 'POLO_EXCELENCIA_COGNITIVA_E_REDACAO_ENEM'
        when score_desempenho_cognitivo_enem >= 74.0
            and score_equidade_acesso_ensino_superior_enem >= 77.0
            then 'REDE_ALTO_DESEMPENHO_ACESSO_SISU_PROUNI'
        when score_desempenho_cognitivo_enem >= 65.0
            then 'DESEMPENHO_INTERMEDIARIO_EM_CONSOLIDACAO'
        else 'TERRITORIO_PRIORITARIO_EQUIDADE_ENSINO_MEDIO'
    end as classificacao_desempenho_enem_municipal,
    reference_date,
    extracted_at
from scored
