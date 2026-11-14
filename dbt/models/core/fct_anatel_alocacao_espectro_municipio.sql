{{ config(materialized='table') }}

with espectro as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        faixa_frequencia_mhz,
        geracao_tecnologica,
        largura_banda_agregada_mhz,
        estacoes_licenciadas_faixa,
        potencia_media_eirp_dbm,
        status_limpeza_faixa,
        reference_date,
        extracted_at
    from {{ ref('stg_anatel_espectro_radiofrequencia') }}
),

totais as (
    select
        id_municipio,
        sum(estacoes_licenciadas_faixa) as total_estacoes_espectro_municipio,
        sum(largura_banda_agregada_mhz) as total_largura_banda_municipio_mhz
    from {{ ref('stg_anatel_espectro_radiofrequencia') }}
    group by id_municipio
)

select
    e.id_municipio,
    e.nome_municipio,
    e.sigla_uf,
    e.faixa_frequencia_mhz,
    e.geracao_tecnologica,
    e.largura_banda_agregada_mhz,
    e.estacoes_licenciadas_faixa,
    round(
        (cast(e.estacoes_licenciadas_faixa as double) / nullif(t.total_estacoes_espectro_municipio, 0)) * 100.0,
        2
    ) as participacao_estacoes_faixa_pct,
    t.total_largura_banda_municipio_mhz,
    e.potencia_media_eirp_dbm,
    e.status_limpeza_faixa,
    e.reference_date,
    e.extracted_at
from espectro e
left join totais t on e.id_municipio = t.id_municipio
