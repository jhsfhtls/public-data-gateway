with aptidao as (
    select * from {{ ref('stg_mapa_zarc_aptidao_climatica') }}
),
proagro as (
    select * from {{ ref('stg_mapa_zarc_proagro_seguro') }}
),
solo as (
    select * from {{ ref('stg_mapa_zarc_risco_solo') }}
)
select
    a.id_municipio,
    a.nome_municipio,
    a.sigla_uf,
    a.id_uf,
    a.regiao,
    a.bioma_predominante,
    a.ano_safra,
    a.qtd_culturas_zoneadas,
    a.decendios_aptos_media,
    a.indice_seguranca_hidrica_safra,
    a.risco_geada_granizo_pct,
    a.indice_aptidao_climatica_zarc,
    p.qtd_operacoes_proagro_safra,
    p.cobertura_seguro_rural_psr_pct,
    p.adesao_zarc_credito_custeio_pct,
    p.taxa_sinistralidade_climatica_pct,
    p.indice_protecao_proagro_psr,
    s.capacidade_retencao_agua_solo_pct,
    s.adocao_plantio_direto_zarc_pct,
    s.estabilidade_pluviometrica_safra_pct,
    s.indice_resiliencia_solo_ad,
    a.reference_date
from aptidao as a
inner join proagro as p
    on a.id_municipio = p.id_municipio
inner join solo as s
    on a.id_municipio = s.id_municipio
