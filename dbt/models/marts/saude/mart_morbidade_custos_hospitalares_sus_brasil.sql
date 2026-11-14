{{ config(materialized='table') }}

with prod as (
    select
        *,
        least(100.0, greatest(0.0, (ticket_medio_aih_brl / 3200.0) * 100.0)) as ticket_norm,
        least(100.0, greatest(0.0, (proporcao_custo_uti_pct / 38.0) * 100.0)) as custo_uti_norm,
        least(100.0, greatest(0.0, (taxa_utilizacao_uti_pct / 16.0) * 100.0)) as uso_uti_norm,
        least(100.0, greatest(0.0, (internacoes_por_1000_hab_ano / 75.0) * 100.0)) as taxa_int_norm,
        least(100.0, greatest(0.0, proporcao_urgencia_pct)) as urg_norm,
        least(100.0, greatest(0.0, (proporcao_icsap_pct / 28.0) * 100.0)) as icsap_norm
    from {{ ref('fct_sih_producao_custos_hospitalares_municipio') }}
),

cid10_agg as (
    select
        id_municipio,
        max(case when capitulo_cid10 = 'Cap_IX_Aparelho_Circulatorio' then internacoes_capitulo_aih else 0 end) as circulatorio_internacoes,
        max(case when capitulo_cid10 = 'Cap_X_Aparelho_Respiratorio' then internacoes_capitulo_aih else 0 end) as respiratorio_internacoes,
        max(case when capitulo_cid10 = 'Cap_II_Neoplasias_Tumores' then internacoes_capitulo_aih else 0 end) as neoplasias_internacoes
    from {{ ref('fct_sih_morbidade_cid10_municipio') }}
    group by id_municipio
),

scored as (
    select
        p.*,
        coalesce(c.circulatorio_internacoes, 0) as circulatorio_internacoes,
        coalesce(c.respiratorio_internacoes, 0) as respiratorio_internacoes,
        coalesce(c.neoplasias_internacoes, 0) as neoplasias_internacoes,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.40 * p.ticket_norm
                    + 0.35 * p.custo_uti_norm
                    + 0.25 * p.uso_uti_norm
                )
            ),
            2
        ) as score_complexidade_assistencial_sih,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.40 * p.taxa_int_norm
                    + 0.35 * p.urg_norm
                    + 0.25 * p.icsap_norm
                )
            ),
            2
        ) as score_pressao_morbidade_hospitalar
    from prod p
    left join cid10_agg c on p.id_municipio = c.id_municipio
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    regiao,
    ano_mes_competencia,
    total_internacoes_aih,
    internacoes_carater_urgencia,
    internacoes_carater_eletivo,
    proporcao_urgencia_pct,
    diarias_hospitalares_totais,
    tempo_medio_permanencia_dias,
    internacoes_por_1000_hab_ano,
    valor_total_aih_brl,
    valor_servicos_hospitalares_brl,
    valor_servicos_profissionais_brl,
    valor_uti_alta_complexidade_brl,
    ticket_medio_aih_brl,
    custo_medio_diaria_brl,
    proporcao_custo_uti_pct,
    altas_hospitalares,
    obitos_hospitalares,
    taxa_mortalidade_hospitalar_pct,
    internacoes_com_uti,
    taxa_utilizacao_uti_pct,
    internacoes_icsap_evitaveis,
    proporcao_icsap_pct,
    circulatorio_internacoes,
    respiratorio_internacoes,
    neoplasias_internacoes,
    score_complexidade_assistencial_sih,
    score_pressao_morbidade_hospitalar,
    case
        when score_complexidade_assistencial_sih >= 78.0 and proporcao_custo_uti_pct >= 28.0
            then 'POLO_CIRURGICO_ALTA_COMPLEXIDADE_NACIONAL'
        when score_complexidade_assistencial_sih >= 62.0 and proporcao_custo_uti_pct >= 20.0
            then 'CENTRO_REFERENCIA_MORBIDADE_REGIONAL'
        when score_complexidade_assistencial_sih >= 45.0
            then 'REDE_HOSPITALAR_MEDIA_COMPLEXIDADE'
        else 'PERFIL_CLINICO_BAIXA_COMPLEXIDADE_ICSAP'
    end as perfil_epidemiologico_hospitalar,
    reference_date,
    extracted_at
from scored
