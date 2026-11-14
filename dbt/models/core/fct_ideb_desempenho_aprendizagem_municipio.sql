{{ config(materialized='table') }}

with resultados as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
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
        reference_date,
        extracted_at
    from {{ ref('stg_ideb_resultados_municipios') }}
),

saeb as (
    select
        id_municipio,
        proficiencia_saeb_lp_5ano,
        proficiencia_saeb_mat_5ano,
        proficiencia_saeb_lp_9ano,
        proficiencia_saeb_mat_9ano,
        aprendizado_adequado_anos_iniciais_pct,
        aprendizado_adequado_anos_finais_pct,
        taxa_participacao_saeb_pct
    from {{ ref('stg_ideb_proficiencia_saeb_aprendizagem') }}
),

evolucao as (
    select
        id_municipio,
        ideb_edicao_anterior_2021,
        variacao_bienal_ideb_pontos,
        gap_meta_anos_iniciais_pontos,
        gap_meta_anos_finais_pontos,
        gap_meta_ensino_medio_pontos,
        gap_rede_privada_vs_publica_em,
        equidade_aprendizagem_inse_ratio
    from {{ ref('stg_ideb_metas_evolucao_bienal') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    r.id_municipio,
    r.nome_municipio,
    r.sigla_uf,
    r.id_uf,
    coalesce(m.nome_uf, r.sigla_uf) as nome_uf,
    coalesce(m.regiao, 'Brasil') as regiao,
    r.cep_8,
    r.ano_edicao_ideb,
    r.ideb_anos_iniciais_rede_publica,
    r.ideb_anos_finais_rede_publica,
    r.ideb_ensino_medio_rede_publica,
    r.ideb_rede_privada_ensino_medio,
    r.indicador_rendimento_fluxo_medio_p,
    r.nota_padronizada_saeb_n_media,
    r.escolas_avaliadas_ideb_municipio,
    r.proporcao_escolas_cumpriram_meta_pct,
    s.proficiencia_saeb_lp_5ano,
    s.proficiencia_saeb_mat_5ano,
    s.proficiencia_saeb_lp_9ano,
    s.proficiencia_saeb_mat_9ano,
    s.aprendizado_adequado_anos_iniciais_pct,
    s.aprendizado_adequado_anos_finais_pct,
    s.taxa_participacao_saeb_pct,
    e.ideb_edicao_anterior_2021,
    e.variacao_bienal_ideb_pontos,
    e.gap_meta_anos_iniciais_pontos,
    e.gap_meta_anos_finais_pontos,
    e.gap_meta_ensino_medio_pontos,
    e.gap_rede_privada_vs_publica_em,
    e.equidade_aprendizagem_inse_ratio,
    r.reference_date,
    r.extracted_at
from resultados r
inner join saeb s on r.id_municipio = s.id_municipio
inner join evolucao e on r.id_municipio = e.id_municipio
left join municipios m on r.id_municipio = m.id_municipio
