{{ config(materialized='table') }}

select
    t.tribunal_sigla,
    t.tribunal_nome,
    t.ramo_justica,
    t.esfera_uf,
    t.porte_tribunal,
    count(p.numero_processo) as total_processos_cadastrados,
    count(case when p.is_processo_ativo then 1 end) as total_processos_ativos,
    count(case when p.categoria_demanda = 'EXECUCAO_FISCAL' then 1 end) as total_execucoes_fiscais,
    count(case when p.categoria_demanda = 'ACAO_TRABALHISTA' then 1 end) as total_acoes_trabalhistas,
    count(case when p.categoria_demanda = 'RECUPERACAO_JUDICIAL' then 1 end) as total_recuperacoes_judiciais,
    coalesce(sum(p.valor_causa), 0.0) as valor_total_contingente,
    coalesce(avg(p.valor_causa), 0.0) as valor_medio_causa,
    max(p.extracted_at) as data_ultima_carga
from {{ ref('dim_tribunal_judiciario') }} t
left join {{ ref('fct_processo_judicial') }} p
    on t.tribunal_sigla = p.tribunal_sigla
group by
    t.tribunal_sigla,
    t.tribunal_nome,
    t.ramo_justica,
    t.esfera_uf,
    t.porte_tribunal
