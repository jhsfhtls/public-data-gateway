{{ config(materialized='table') }}

with srag as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
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
        reference_date,
        extracted_at
    from {{ ref('stg_vigilancia_sindromes_respiratorias_srag_municipio') }}
),

sipni as (
    select
        id_municipio,
        doses_aplicadas_calendario_pni_ano,
        salas_vacina_ativas_cnes,
        cobertura_pentavalente_d3_pct,
        cobertura_poliomielite_vip_pct,
        cobertura_triplice_viral_d1_pct,
        homogeneidade_cobertura_vacinal_pni_pct,
        taxa_abandono_esquema_multidose_pct
    from {{ ref('stg_vigilancia_cobertura_vacinal_sipni_municipio') }}
),

cievs as (
    select
        id_municipio,
        unidades_sentinela_vigilancia_ativas,
        notificacoes_imediatas_24h_cievs_pct,
        investigacoes_epidemiologicas_48h_pct,
        tempo_medio_liberacao_gal_lacen_horas,
        indice_bloqueio_vetorial_arboviroses_pct,
        surtos_investigados_encerrados_pct,
        integracao_rnds_esus_sinan_pct
    from {{ ref('stg_vigilancia_capacidade_resposta_cievs_lacen') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    s.id_municipio,
    s.nome_municipio,
    s.sigla_uf,
    s.id_uf,
    coalesce(m.nome_uf, s.sigla_uf) as nome_uf,
    coalesce(m.regiao, s.regiao) as regiao,
    s.ano_epidemiologico,
    s.notificacoes_sindrome_gripal_esus_ano,
    s.internacoes_srag_sivep_ano,
    s.taxa_incidencia_srag_100k_hab,
    s.internacoes_srag_uti_ano,
    s.proporcao_srag_uti_pct,
    s.obitos_srag_confirmados_ano,
    s.letalidade_hospitalar_srag_pct,
    s.confirmacao_laboratorial_rt_pcr_srag_pct,
    v.doses_aplicadas_calendario_pni_ano,
    v.salas_vacina_ativas_cnes,
    v.cobertura_pentavalente_d3_pct,
    v.cobertura_poliomielite_vip_pct,
    v.cobertura_triplice_viral_d1_pct,
    v.homogeneidade_cobertura_vacinal_pni_pct,
    v.taxa_abandono_esquema_multidose_pct,
    c.unidades_sentinela_vigilancia_ativas,
    c.notificacoes_imediatas_24h_cievs_pct,
    c.investigacoes_epidemiologicas_48h_pct,
    c.tempo_medio_liberacao_gal_lacen_horas,
    c.indice_bloqueio_vetorial_arboviroses_pct,
    c.surtos_investigados_encerrados_pct,
    c.integracao_rnds_esus_sinan_pct,
    s.reference_date,
    s.extracted_at
from srag s
inner join sipni v on s.id_municipio = v.id_municipio
inner join cievs c on s.id_municipio = c.id_municipio
left join municipios m on s.id_municipio = m.id_municipio
