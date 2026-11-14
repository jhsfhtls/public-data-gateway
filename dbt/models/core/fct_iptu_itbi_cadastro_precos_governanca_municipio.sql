{{ config(materialized='table') }}

with cad as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        regiao,
        ano_exercicio_fiscal,
        inscricoes_imobiliarias_ativas_iptu,
        area_construida_tributada_mil_m2,
        valor_venal_total_bilhoes_brl,
        valor_venal_medio_m2_brl,
        aliquota_media_efetiva_iptu_pct,
        arrecadacao_iptu_milhoes_brl,
        taxa_adimplencia_iptu_pct,
        indice_atualizacao_pgv_pct,
        reference_date,
        extracted_at
    from {{ ref('stg_iptu_itbi_cadastro_imobiliario_municipal') }}
),

din as (
    select
        id_municipio,
        preco_medio_m2_itbi_declarado_brl,
        variacao_anual_preco_m2_itbi_pct,
        razao_venal_pgv_sobre_mercado_itbi,
        participacao_lancamentos_novos_pct,
        concentracao_uso_residencial_pct,
        indice_liquidez_imobiliaria_itbi_pct,
        taxa_vacancia_cadastral_estimada_pct
    from {{ ref('stg_iptu_itbi_dinamica_precos_bairros') }}
),

gov as (
    select
        id_municipio,
        cobertura_cadastral_georreferenciada_ctm_pct,
        transacoes_itbi_emissao_digital_pct,
        integracao_iptu_sinter_cno_onr_pct,
        arrecadacao_itbi_milhoes_brl,
        participacao_iptu_itbi_receita_propria_pct,
        receita_imobiliaria_per_capita_brl,
        nivel_transparencia_dados_abertos_pgv
    from {{ ref('stg_iptu_itbi_governanca_fiscal_geoespacial') }}
),

municipios as (
    select
        id_municipio,
        nome_uf
    from {{ ref('dim_municipio') }}
)

select
    c.id_municipio,
    c.nome_municipio,
    c.sigla_uf,
    c.id_uf,
    m.nome_uf,
    c.regiao,
    c.ano_exercicio_fiscal,
    c.inscricoes_imobiliarias_ativas_iptu,
    c.area_construida_tributada_mil_m2,
    c.valor_venal_total_bilhoes_brl,
    c.valor_venal_medio_m2_brl,
    c.aliquota_media_efetiva_iptu_pct,
    c.arrecadacao_iptu_milhoes_brl,
    c.taxa_adimplencia_iptu_pct,
    c.indice_atualizacao_pgv_pct,
    d.preco_medio_m2_itbi_declarado_brl,
    d.variacao_anual_preco_m2_itbi_pct,
    d.razao_venal_pgv_sobre_mercado_itbi,
    d.participacao_lancamentos_novos_pct,
    d.concentracao_uso_residencial_pct,
    d.indice_liquidez_imobiliaria_itbi_pct,
    d.taxa_vacancia_cadastral_estimada_pct,
    g.cobertura_cadastral_georreferenciada_ctm_pct,
    g.transacoes_itbi_emissao_digital_pct,
    g.integracao_iptu_sinter_cno_onr_pct,
    g.arrecadacao_itbi_milhoes_brl,
    g.participacao_iptu_itbi_receita_propria_pct,
    g.receita_imobiliaria_per_capita_brl,
    g.nivel_transparencia_dados_abertos_pgv,
    c.reference_date,
    c.extracted_at
from cad c
inner join din d on c.id_municipio = d.id_municipio
inner join gov g on c.id_municipio = g.id_municipio
inner join municipios m on c.id_municipio = m.id_municipio
