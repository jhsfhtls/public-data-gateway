{{ config(materialized='table') }}

with sinan_agg as (
    select
        id_municipio,
        sum(casos_notificados_ano) as total_casos_notificados_sinan_ano,
        sum(casos_confirmados_ano) as total_casos_confirmados_sinan_ano,
        max(
            case
                when grupo_agravo_sinan = 'Arboviroses_Dengue_Chikungunya_Zika'
                    then taxa_incidencia_confirmada_100k_hab
            end
        ) as incidencia_arboviroses_100k_hab,
        round(avg(proporcao_encerramento_oportuno_sinan_pct), 2)
            as proporcao_encerramento_oportuno_sinan_pct
    from {{ ref('fct_vigilancia_agravos_sinan_municipio') }}
    group by id_municipio
),

joined as (
    select
        f.*,
        sa.total_casos_notificados_sinan_ano,
        sa.total_casos_confirmados_sinan_ano,
        sa.incidencia_arboviroses_100k_hab,
        sa.proporcao_encerramento_oportuno_sinan_pct
    from {{ ref('fct_vigilancia_srag_imunizacao_cievs_municipio') }} f
    inner join sinan_agg sa on f.id_municipio = sa.id_municipio
),

scored as (
    select
        *,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.25 * confirmacao_laboratorial_rt_pcr_srag_pct
                    + 0.25 * proporcao_encerramento_oportuno_sinan_pct
                    + 0.25 * notificacoes_imediatas_24h_cievs_pct
                    + 0.25 * integracao_rnds_esus_sinan_pct
                )
            ),
            2
        ) as score_prontidao_vigilancia_epidemiologica,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.35 * cobertura_pentavalente_d3_pct
                    + 0.35 * cobertura_triplice_viral_d1_pct
                    + 0.30 * homogeneidade_cobertura_vacinal_pni_pct
                )
            ),
            2
        ) as score_protecao_imunizacao_coletiva_pni
    from joined
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    regiao,
    ano_epidemiologico,
    notificacoes_sindrome_gripal_esus_ano,
    internacoes_srag_sivep_ano,
    taxa_incidencia_srag_100k_hab,
    internacoes_srag_uti_ano,
    proporcao_srag_uti_pct,
    obitos_srag_confirmados_ano,
    letalidade_hospitalar_srag_pct,
    confirmacao_laboratorial_rt_pcr_srag_pct,
    total_casos_notificados_sinan_ano,
    total_casos_confirmados_sinan_ano,
    incidencia_arboviroses_100k_hab,
    proporcao_encerramento_oportuno_sinan_pct,
    doses_aplicadas_calendario_pni_ano,
    salas_vacina_ativas_cnes,
    cobertura_pentavalente_d3_pct,
    cobertura_poliomielite_vip_pct,
    cobertura_triplice_viral_d1_pct,
    homogeneidade_cobertura_vacinal_pni_pct,
    taxa_abandono_esquema_multidose_pct,
    unidades_sentinela_vigilancia_ativas,
    notificacoes_imediatas_24h_cievs_pct,
    investigacoes_epidemiologicas_48h_pct,
    tempo_medio_liberacao_gal_lacen_horas,
    indice_bloqueio_vetorial_arboviroses_pct,
    surtos_investigados_encerrados_pct,
    integracao_rnds_esus_sinan_pct,
    score_prontidao_vigilancia_epidemiologica,
    score_protecao_imunizacao_coletiva_pni,
    case
        when score_prontidao_vigilancia_epidemiologica >= 88.0
            and score_protecao_imunizacao_coletiva_pni >= 86.0
            then 'POLO_SENTINELA_ALTA_COBERTURA_IMUNIZACAO'
        when score_prontidao_vigilancia_epidemiologica >= 82.0
            and score_protecao_imunizacao_coletiva_pni >= 80.0
            then 'REDE_VIGILANCIA_EPIDEMIOLOGICA_CONSOLIDADA'
        when score_prontidao_vigilancia_epidemiologica >= 75.0
            then 'TERRITORIO_PRESSAO_ENDEMICA_EM_MONITORAMENTO'
        else 'ZONA_PRIORITARIA_CONTROLE_ARBOVIROSES_E_PNI'
    end as classificacao_vigilancia_epidemiologica_municipal,
    reference_date,
    extracted_at
from scored
