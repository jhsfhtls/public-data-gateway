{{ config(materialized='table') }}

with usinas as (
    select
        id_municipio,
        count(*) as qtd_complexos_geracao_ons,
        round(sum(geracao_media_verificada_mwmed), 2) as geracao_verificada_total_mwmed
    from {{ ref('stg_ons_capacidade_geracao_municipio') }}
    group by id_municipio
),

bal as (
    select
        id_municipio,
        ear_energia_armazenada_pct,
        cmo_medio_brl_mwh
    from {{ ref('stg_ons_balanco_carga_subsistema_uf') }}
),

grid as (
    select
        id_municipio,
        subestacoes_alta_tensao,
        linhas_transmissao_km,
        indice_continuidade_dec_horas,
        disponibilidade_rede_sin_pct
    from {{ ref('stg_mme_intercambio_seguranca_energetica') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.subsistema_sin,
    r.potencia_instalada_sin_mw,
    r.geracao_renovavel_sin_pct,
    r.carga_energia_mwmed,
    r.fator_capacidade_renovavel_pct,
    coalesce(u.qtd_complexos_geracao_ons, 0) as qtd_complexos_geracao_ons,
    coalesce(u.geracao_verificada_total_mwmed, 0.0) as geracao_verificada_total_mwmed,
    coalesce(b.ear_energia_armazenada_pct, 70.0) as ear_energia_armazenada_pct,
    coalesce(b.cmo_medio_brl_mwh, 100.0) as cmo_medio_brl_mwh,
    coalesce(g.subestacoes_alta_tensao, 10) as subestacoes_alta_tensao,
    coalesce(g.linhas_transmissao_km, 350.0) as linhas_transmissao_km,
    coalesce(g.indice_continuidade_dec_horas, 10.0) as indice_continuidade_dec_horas,
    coalesce(g.disponibilidade_rede_sin_pct, 99.0) as disponibilidade_rede_sin_pct
from {{ ref('stg_ons_energia_resumo_municipal') }} r
left join usinas u on r.id_municipio = u.id_municipio
left join bal b on r.id_municipio = b.id_municipio
left join grid g on r.id_municipio = g.id_municipio
