{{ config(materialized='table') }}

with corredores as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        ano_referencia,
        rodovias_federais_principais,
        extensao_malha_federal_km,
        trechos_concedidos_antt_km,
        proporcao_malha_concedida_pct,
        pracas_pedagio_ativas,
        volume_medio_diario_vmd_pesados,
        fluxo_anual_cargas_estimado_ton,
        reference_date,
        extracted_at
    from {{ ref('stg_antt_corredores_rodoviarios') }}
),

rntrc as (
    select
        id_municipio,
        total_transportadores_rntrc,
        transportadores_etc_empresas,
        transportadores_tac_autonomos,
        transportadores_ctc_cooperativas,
        frota_veiculos_habilitados,
        capacidade_estatica_total_ton,
        idade_media_frota_anos
    from {{ ref('stg_antt_frota_rntrc_municipio') }}
),

ferrovia as (
    select
        id_municipio,
        possui_conexao_ferroviaria,
        concessionarias_ferroviarias,
        extensao_ferroviaria_km,
        terminais_carga_intermodal,
        movimentacao_ferroviaria_anual_tu,
        principais_mercadorias_ferrovia
    from {{ ref('stg_antt_malha_ferroviaria_terminais') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    c.id_municipio,
    c.nome_municipio,
    c.sigla_uf,
    c.id_uf,
    coalesce(m.nome_uf, c.sigla_uf) as nome_uf,
    coalesce(m.regiao, 'Brasil') as regiao,
    c.ano_referencia,
    c.rodovias_federais_principais,
    c.extensao_malha_federal_km,
    c.trechos_concedidos_antt_km,
    c.proporcao_malha_concedida_pct,
    c.pracas_pedagio_ativas,
    c.volume_medio_diario_vmd_pesados,
    c.fluxo_anual_cargas_estimado_ton,
    r.total_transportadores_rntrc,
    r.transportadores_etc_empresas,
    r.transportadores_tac_autonomos,
    r.transportadores_ctc_cooperativas,
    round(
        (cast(r.transportadores_etc_empresas as double) / nullif(r.total_transportadores_rntrc, 0)) * 100.0,
        2
    ) as proporcao_etc_empresas_pct,
    r.frota_veiculos_habilitados,
    r.capacidade_estatica_total_ton,
    round(
        cast(r.capacidade_estatica_total_ton as double) / nullif(r.frota_veiculos_habilitados, 0),
        2
    ) as capacidade_media_veiculo_ton,
    r.idade_media_frota_anos,
    f.possui_conexao_ferroviaria,
    f.concessionarias_ferroviarias,
    f.extensao_ferroviaria_km,
    f.terminais_carga_intermodal,
    f.movimentacao_ferroviaria_anual_tu,
    f.principais_mercadorias_ferrovia,
    c.reference_date,
    c.extracted_at
from corredores c
inner join rntrc r on c.id_municipio = r.id_municipio
inner join ferrovia f on c.id_municipio = f.id_municipio
left join municipios m on c.id_municipio = m.id_municipio
