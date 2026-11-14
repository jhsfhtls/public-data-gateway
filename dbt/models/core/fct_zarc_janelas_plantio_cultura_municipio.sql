select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    bioma_predominante,
    cultura_zarc,
    grupo_solo_ad,
    risco_climatico_nivel_pct,
    decendios_aptos_plantio,
    janela_plantio_inicio_mes,
    janela_plantio_fim_mes,
    elegivel_proagro_psr_pct,
    reference_date
from {{ ref('stg_mapa_zarc_janelas_plantio') }}
