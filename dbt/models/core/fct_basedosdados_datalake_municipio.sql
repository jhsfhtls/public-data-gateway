with cob as (
    select * from {{ ref('stg_bd_datalake_cobertura') }}
),
gov as (
    select * from {{ ref('stg_bd_governanca_metadados') }}
),
con as (
    select * from {{ ref('stg_bd_consumo_federado') }}
)
select
    c.id_municipio,
    c.nome_municipio,
    c.sigla_uf,
    c.id_uf,
    c.regiao,
    c.qtd_datasets_tratados_bd,
    c.qtd_tabelas_bigquery_municipio,
    c.completude_chaves_ibge_pct,
    c.taxa_atualizacao_mensal_pct,
    c.indice_cobertura_datalake_bd,
    g.dicionario_padronizado_pct,
    g.conformidade_chaves_canonicas_pct,
    g.disponibilidade_bq_sql_pct,
    g.indice_governanca_bd,
    q.consultas_bigquery_mensais,
    q.eficiencia_particionamento_pct,
    q.custo_medio_scan_mb,
    q.taxa_reuso_cross_domain_pct,
    q.indice_eficiencia_federada_bd,
    c.reference_date
from cob as c
inner join gov as g
    on c.id_municipio = g.id_municipio
inner join con as q
    on c.id_municipio = q.id_municipio
