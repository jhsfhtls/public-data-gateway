{{ config(materialized='table') }}

with infra as (
    select
        *,
        least(100.0, greatest(0.0, (leitos_por_1000_hab / 4.0) * 100.0)) as leitos_norm,
        least(100.0, greatest(0.0, (leitos_uti_por_10k_hab / 6.0) * 100.0)) as uti_norm,
        least(100.0, greatest(0.0, (cast(hospitais_gerais_especializados as double) / 80.0) * 100.0)) as hosp_norm
    from {{ ref('fct_cnes_infraestrutura_hospitalar_municipio') }}
),

workforce_agg as (
    select
        id_municipio,
        max(case when categoria_profissional = 'Medicos' then total_vinculos_ativos else 0 end) as medicos_ativos,
        max(case when categoria_profissional = 'Medicos' then densidade_por_10k_hab else 0.0 end) as medicos_por_10k_hab,
        max(case when categoria_profissional = 'Enfermeiros' then total_vinculos_ativos else 0 end) as enfermeiros_ativos,
        max(case when categoria_profissional = 'Enfermeiros' then densidade_por_10k_hab else 0.0 end) as enfermeiros_por_10k_hab,
        max(case when categoria_profissional = 'Equipes_Saude_Familia_eSF' then total_vinculos_ativos else 0 end) as equipes_esf_ativas
    from {{ ref('fct_cnes_forca_trabalho_saude_municipio') }}
    group by id_municipio
),

scored_uti as (
    select
        i.*,
        coalesce(w.medicos_ativos, 0) as medicos_ativos,
        coalesce(w.medicos_por_10k_hab, 0.0) as medicos_por_10k_hab,
        coalesce(w.enfermeiros_ativos, 0) as enfermeiros_ativos,
        coalesce(w.enfermeiros_por_10k_hab, 0.0) as enfermeiros_por_10k_hab,
        coalesce(w.equipes_esf_ativas, 0) as equipes_esf_ativas,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.50 * i.uti_norm
                    + 0.35 * i.indice_disponibilidade_diagnostica
                    + 0.15 * i.hosp_norm
                )
            ),
            2
        ) as score_alta_complexidade_uti
    from infra i
    left join workforce_agg w on i.id_municipio = w.id_municipio
),

final_scores as (
    select
        *,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.30 * leitos_norm
                    + 0.30 * score_alta_complexidade_uti
                    + 0.20 * proporcao_rede_sus_pct
                    + 0.20 * indice_disponibilidade_diagnostica
                )
            ),
            2
        ) as score_capacidade_hospitalar
    from scored_uti
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    regiao,
    ano_mes_competencia,
    codigo_cnes_referencia,
    cnpj_14,
    cep_8,
    estabelecimento_referencia_nome,
    total_estabelecimentos_ativos,
    hospitais_gerais_especializados,
    unidades_basicas_saude_ubs,
    unidades_pronto_atendimento_upa,
    estabelecimentos_atendimento_sus,
    proporcao_rede_sus_pct,
    total_leitos_hospitalares,
    leitos_sus,
    leitos_nao_sus,
    proporcao_leitos_sus_pct,
    total_leitos_uti,
    leitos_uti_adulto,
    leitos_uti_pediatrico_neonatal,
    leitos_uti_sus,
    leitos_por_1000_hab,
    leitos_uti_por_10k_hab,
    tomografos_computadorizados,
    ressonancia_magnetica,
    mamografos_em_uso,
    aparelhos_hemodialise,
    aparelhos_raio_x,
    proporcao_equipamentos_sus_pct,
    indice_disponibilidade_diagnostica,
    medicos_ativos,
    medicos_por_10k_hab,
    enfermeiros_ativos,
    enfermeiros_por_10k_hab,
    equipes_esf_ativas,
    score_alta_complexidade_uti,
    score_capacidade_hospitalar,
    case
        when total_leitos_uti >= 500 and score_capacidade_hospitalar >= 78.0 and score_alta_complexidade_uti >= 72.0
            then 'POLO_MACRORREGIONAL_ALTA_COMPLEXIDADE'
        when total_leitos_uti >= 200 and score_capacidade_hospitalar >= 62.0
            then 'CENTRO_REFERENCIA_HOSPITALAR_REGIONAL'
        when score_capacidade_hospitalar >= 45.0
            then 'REDE_ASSISTENCIAL_SECUNDARIA'
        else 'ATENCAO_BASICA_PREDOMINANTE'
    end as classificacao_rede_assistencial,
    reference_date,
    extracted_at
from final_scores
