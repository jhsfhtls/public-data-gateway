{{ config(materialized='table') }}

with estacoes as (
    select * from {{ ref('stg_ana_hidroweb_estacoes_telemetricas') }}
),

pluv as (
    select * from {{ ref('stg_ana_hidroweb_pluviometria_alertas') }}
),

qualidade as (
    select * from {{ ref('stg_ana_hidroweb_qualidade_seguranca_hidrica') }}
),

cotas_agg as (
    select
        id_municipio,
        cast(round(cast(avg(vazao_media_m3s) as decimal(10, 4)), 2) as double) as vazao_media_global_m3s,
        cast(round(cast(avg(cota_media_cm) as decimal(10, 4)), 2) as double) as cota_media_global_cm,
        cast(round(cast(avg(taxa_eventos_criticos_pct) as decimal(10, 4)), 2) as double) as taxa_eventos_criticos_media_pct,
        cast(round(cast(avg(capacidade_regularizacao_pct) as decimal(10, 4)), 2) as double) as capacidade_regularizacao_media_pct
    from {{ ref('stg_ana_hidroweb_cotas_vazoes_bacia') }}
    group by id_municipio
)

select
    e.id_municipio,
    e.nome_municipio,
    e.sigla_uf,
    e.id_uf,
    e.regiao,
    e.regiao_hidrografica,
    e.ano_referencia,
    e.qtd_estacoes_telemetricas_ativas,
    e.qtd_estacoes_fluviometricas,
    e.qtd_estacoes_pluviometricas,
    e.disponibilidade_telemetria_pct,
    e.densidade_rede_estacoes_1000km2,
    e.indice_cobertura_rede_hidroweb,
    c.vazao_media_global_m3s,
    c.cota_media_global_cm,
    c.taxa_eventos_criticos_media_pct,
    c.capacidade_regularizacao_media_pct,
    p.precipitacao_acumulada_anual_mm,
    p.anomalia_precipitacao_pct,
    p.dias_consecutivos_sem_chuva,
    p.eventos_alerta_cheia,
    p.eventos_alerta_estiagem,
    p.indice_equilibrio_pluviometrico,
    q.indice_qualidade_agua_iqa,
    q.indice_seguranca_hidrica_ish,
    q.comprometimento_balanco_hidrico_pct,
    q.cobertura_outorga_regularizada_pct,
    q.confiabilidade_serie_historica_pct,
    q.indice_resiliencia_hidrica,
    e.reference_date
from estacoes as e
inner join pluv as p
    on e.id_municipio = p.id_municipio
inner join qualidade as q
    on e.id_municipio = q.id_municipio
inner join cotas_agg as c
    on e.id_municipio = c.id_municipio
