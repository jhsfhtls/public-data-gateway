{{ config(materialized='table') }}

with sin as (
    select
        id_municipio,
        count(*) as qtd_agrupamentos_sinistro_prf
    from {{ ref('stg_prf_sinistros_rodovias_br') }}
    group by id_municipio
),

cor as (
    select
        id_municipio,
        extensao_monitorada_km,
        sinistros_carga_pesada_ano,
        indice_severidade_corredor
    from {{ ref('stg_prf_corredores_logisticos_trechos') }}
),

cau as (
    select
        id_municipio,
        pct_colisao_frontal_traseira,
        pct_envolvimento_caminhao_onibus,
        pct_condicao_clima_adverso,
        alcoolemia_autuacoes_ano
    from {{ ref('stg_prf_perfil_causas_condicoes') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.rodovia_br_principal,
    r.total_sinistros_prf_ano,
    r.total_feridos_prf_ano,
    r.total_obitos_prf_ano,
    r.taxa_letalidade_sinistro_pct,
    coalesce(s.qtd_agrupamentos_sinistro_prf, 0) as qtd_agrupamentos_sinistro_prf,
    coalesce(c.extensao_monitorada_km, 0.0) as extensao_monitorada_km,
    coalesce(c.sinistros_carga_pesada_ano, 0) as sinistros_carga_pesada_ano,
    coalesce(c.indice_severidade_corredor, 3.0) as indice_severidade_corredor,
    coalesce(p.pct_colisao_frontal_traseira, 0.0) as pct_colisao_frontal_traseira,
    coalesce(p.pct_envolvimento_caminhao_onibus, 0.0) as pct_envolvimento_caminhao_onibus,
    coalesce(p.pct_condicao_clima_adverso, 0.0) as pct_condicao_clima_adverso,
    coalesce(p.alcoolemia_autuacoes_ano, 0) as alcoolemia_autuacoes_ano
from {{ ref('stg_prf_sinistralidade_resumo_municipal') }} r
left join sin s on r.id_municipio = s.id_municipio
left join cor c on r.id_municipio = c.id_municipio
left join cau p on r.id_municipio = p.id_municipio
