{{ config(materialized='table') }}

with proc as (
    select
        id_municipio,
        count(*) as qtd_registros_amostra_sigmine,
        round(sum(arrecadacao_cfem_anual_mil_brl), 2) as cfem_amostra_processos_mil_brl
    from {{ ref('stg_anm_sigmine_processos_municipio') }}
    group by id_municipio
),

cfem as (
    select
        id_municipio,
        substancia_principal,
        valor_operacao_mineral_mil_brl,
        aliquota_media_cfem_pct
    from {{ ref('stg_anm_cfem_arrecadacao_royalties') }}
),

sigbm as (
    select
        id_municipio,
        qtd_barragens_sigbm,
        barragens_paebm_emergencia,
        nivel_dano_potencial_associado
    from {{ ref('stg_anm_sigbm_barragens_risco_esg') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.processos_ativos_sigmine,
    r.area_total_titulada_ha,
    r.arrecadacao_cfem_total_mil_brl,
    r.indice_conformidade_sigbm_pct,
    r.participacao_substancias_criticas_pct,
    coalesce(p.qtd_registros_amostra_sigmine, 0) as qtd_registros_amostra_sigmine,
    coalesce(p.cfem_amostra_processos_mil_brl, 0.0) as cfem_amostra_processos_mil_brl,
    coalesce(c.substancia_principal, 'NAO_INFORMADO') as substancia_principal,
    coalesce(c.valor_operacao_mineral_mil_brl, 0.0) as valor_operacao_mineral_mil_brl,
    coalesce(c.aliquota_media_cfem_pct, 2.0) as aliquota_media_cfem_pct,
    coalesce(s.qtd_barragens_sigbm, 0) as qtd_barragens_sigbm,
    coalesce(s.barragens_paebm_emergencia, 0) as barragens_paebm_emergencia,
    coalesce(s.nivel_dano_potencial_associado, 'SEM_BARRAGEM') as nivel_dano_potencial_associado
from {{ ref('stg_anm_mineracao_resumo_municipal') }} r
left join proc p on r.id_municipio = p.id_municipio
left join cfem c on r.id_municipio = c.id_municipio
left join sigbm s on r.id_municipio = s.id_municipio
