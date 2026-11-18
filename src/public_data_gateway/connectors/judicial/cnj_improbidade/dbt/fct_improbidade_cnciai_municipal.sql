{{ config(materialized='table') }}

with resumo as (
    select * from {{ ref('stg_cnciai_resumo_municipal') }}
),

condenacoes as (
    select
        id_municipio,
        count(*) as qtd_registros_detalhados_condenacao,
        sum(valor_ressarcimento_multa_brl) as soma_condenacoes_brl
    from {{ ref('stg_cnciai_condenacoes') }}
    group by id_municipio
),

sancoes as (
    select
        id_municipio,
        max(prazo_proibicao_anos) as maior_prazo_proibicao_anos
    from {{ ref('stg_cnciai_sancoes_contratar') }}
    group by id_municipio
),

ressarc as (
    select
        id_municipio,
        sum(valor_ressarcimento_brl) as soma_ressarcimento_principal_brl,
        sum(valor_multa_civil_brl) as soma_multas_civis_brl
    from {{ ref('stg_cnciai_ressarcimento_erario') }}
    group by id_municipio
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.total_condenacoes_ativas,
    r.total_proibidos_contratar_pj,
    r.total_suspensao_direitos_politicos,
    r.valor_total_ressarcimento_multa_brl,
    r.taxa_proibicao_contratar_pct,
    coalesce(c.qtd_registros_detalhados_condenacao, 0) as qtd_registros_detalhados_condenacao,
    coalesce(s.maior_prazo_proibicao_anos, 0) as maior_prazo_proibicao_anos,
    coalesce(re.soma_ressarcimento_principal_brl, 0.0) as soma_ressarcimento_principal_brl,
    coalesce(re.soma_multas_civis_brl, 0.0) as soma_multas_civis_brl,
    round(coalesce(c.soma_condenacoes_brl, 0.0), 2) as soma_auditada_condenacoes_brl
from resumo r
left join condenacoes c on r.id_municipio = c.id_municipio
left join sancoes s on r.id_municipio = s.id_municipio
left join ressarc re on r.id_municipio = re.id_municipio
