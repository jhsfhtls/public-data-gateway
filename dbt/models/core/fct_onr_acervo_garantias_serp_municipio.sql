{{ config(materialized='table') }}

with acervo as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        cep_8,
        competencia_referencia,
        serventias_ri_integradas_srei,
        total_matriculas_ativas_cnm,
        matriculas_urbanas_ativas,
        matriculas_rurais_georreferenciadas,
        taxa_conversao_cnm_digital_pct,
        novas_matriculas_abertas_ano,
        certidoes_digitais_srei_ano,
        visualizacoes_matricula_online_ano,
        reference_date,
        extracted_at
    from {{ ref('stg_onr_matriculas_acervo_municipio') }}
),

garantias as (
    select
        id_municipio,
        contratos_alienacao_fiduciaria_ano,
        volume_credito_imobiliario_garantido_milhoes_brl,
        participacao_sfi_sbpe_pmcmv_pct,
        contratos_home_equity_cgi_ano,
        intimacoes_purgacao_mora_fiduciaria_ano,
        consolidacoes_propriedade_inadimplencia_ano,
        taxa_execucao_garantia_fiduciaria_pct
    from {{ ref('stg_onr_garantias_credito_imobiliario') }}
),

serp as (
    select
        id_municipio,
        indice_integracao_serp_saec_pct,
        proporcao_titulos_nato_digitais_extrato_pct,
        prazo_medio_qualificacao_registral_dias,
        prazo_medio_certidao_eletronica_horas,
        ordens_indisponibilidade_cnib_processadas_ano,
        penhoras_online_arisp_srei_ano,
        taxa_nota_devolutiva_exigencia_pct
    from {{ ref('stg_onr_interoperabilidade_serp_eficiencia') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    a.id_municipio,
    a.nome_municipio,
    a.sigla_uf,
    a.id_uf,
    coalesce(m.nome_uf, a.sigla_uf) as nome_uf,
    coalesce(m.regiao, 'Brasil') as regiao,
    a.cep_8,
    a.competencia_referencia,
    a.serventias_ri_integradas_srei,
    a.total_matriculas_ativas_cnm,
    a.matriculas_urbanas_ativas,
    a.matriculas_rurais_georreferenciadas,
    a.taxa_conversao_cnm_digital_pct,
    a.novas_matriculas_abertas_ano,
    a.certidoes_digitais_srei_ano,
    a.visualizacoes_matricula_online_ano,
    g.contratos_alienacao_fiduciaria_ano,
    g.volume_credito_imobiliario_garantido_milhoes_brl,
    g.participacao_sfi_sbpe_pmcmv_pct,
    g.contratos_home_equity_cgi_ano,
    g.intimacoes_purgacao_mora_fiduciaria_ano,
    g.consolidacoes_propriedade_inadimplencia_ano,
    g.taxa_execucao_garantia_fiduciaria_pct,
    s.indice_integracao_serp_saec_pct,
    s.proporcao_titulos_nato_digitais_extrato_pct,
    s.prazo_medio_qualificacao_registral_dias,
    s.prazo_medio_certidao_eletronica_horas,
    s.ordens_indisponibilidade_cnib_processadas_ano,
    s.penhoras_online_arisp_srei_ano,
    s.taxa_nota_devolutiva_exigencia_pct,
    a.reference_date,
    a.extracted_at
from acervo a
inner join garantias g on a.id_municipio = g.id_municipio
inner join serp s on a.id_municipio = s.id_municipio
left join municipios m on a.id_municipio = m.id_municipio
