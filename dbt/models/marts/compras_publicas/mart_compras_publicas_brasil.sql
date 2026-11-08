{{ config(materialized='table') }}

with contratacoes as (
    select * from {{ ref('fct_contratacao_publica') }}
)

select
    coalesce(sigla_uf, 'ND') as sigla_uf,
    esfera_id,
    modalidade_nome,
    ano_compra,
    count(distinct numero_controle_pncp) as total_contratacoes,
    count(distinct case when is_srp then numero_controle_pncp end) as total_contratacoes_srp,
    coalesce(round(sum(valor_total_estimado), 2), 0.0) as valor_total_estimado,
    coalesce(round(sum(valor_total_homologado), 2), 0.0) as valor_total_homologado,
    coalesce(round(sum(economia_estimada_homologada_absoluta), 2), 0.0) as economia_estimada_homologada_absoluta,
    coalesce(
        round(
            (sum(economia_estimada_homologada_absoluta) * 100.0) / nullif(sum(valor_total_estimado), 0),
            2
        ),
        0.0
    ) as taxa_economia_licitatoria_pct,
    coalesce(
        round(
            sum(valor_total_homologado) / nullif(count(distinct numero_controle_pncp), 0),
            2
        ),
        0.0
    ) as ticket_medio_contratacao,
    count(distinct cnpj_orgao) as total_orgaos_distintos
from contratacoes
group by
    coalesce(sigla_uf, 'ND'),
    esfera_id,
    modalidade_nome,
    ano_compra
