{{ config(materialized='table') }}

with trib as (
    select
        id_municipio,
        count(*) as qtd_blocos_arrecadacao_rfb,
        round(sum(arrecadacao_federal_anual_mi_brl), 2) as soma_blocos_arrecadacao_mi_brl
    from {{ ref('stg_rfb_arrecadacao_tributos_federais') }}
    group by id_municipio
),

pgfn as (
    select
        id_municipio,
        estoque_divida_ativa_uf_mi_brl,
        devedores_previdenciarios_pj,
        recuperacao_creditos_anual_mi_brl,
        indice_recuperacao_pgfn_pct
    from {{ ref('stg_rfb_divida_ativa_pgfn_estado') }}
),

fpm as (
    select
        id_municipio,
        transferencias_ipi_exportacao_mi_brl,
        cota_simples_nacional_munic_mi_brl
    from {{ ref('stg_rfb_repasses_fpm_royalties') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.arrecadacao_federal_total_mi_brl,
    r.contribuintes_pj_ativos_municipio,
    r.inscritos_divida_ativa_pgfn_mi_brl,
    r.repasses_fpm_anual_mi_brl,
    r.crescimento_real_arrecadacao_pct,
    coalesce(tr.qtd_blocos_arrecadacao_rfb, 2) as qtd_blocos_arrecadacao_rfb,
    coalesce(tr.soma_blocos_arrecadacao_mi_brl, r.arrecadacao_federal_total_mi_brl) as soma_blocos_arrecadacao_mi_brl,
    coalesce(pg.estoque_divida_ativa_uf_mi_brl, 50000.0) as estoque_divida_ativa_uf_mi_brl,
    coalesce(pg.devedores_previdenciarios_pj, 10000) as devedores_previdenciarios_pj,
    coalesce(pg.recuperacao_creditos_anual_mi_brl, 2000.0) as recuperacao_creditos_anual_mi_brl,
    coalesce(pg.indice_recuperacao_pgfn_pct, 12.0) as indice_recuperacao_pgfn_pct,
    coalesce(fp.transferencias_ipi_exportacao_mi_brl, 200.0) as transferencias_ipi_exportacao_mi_brl,
    coalesce(fp.cota_simples_nacional_munic_mi_brl, 1500.0) as cota_simples_nacional_munic_mi_brl
from {{ ref('stg_rfb_arrecadacao_resumo_municipal') }} r
left join trib tr on r.id_municipio = tr.id_municipio
left join pgfn pg on r.id_municipio = pg.id_municipio
left join fpm fp on r.id_municipio = fp.id_municipio
