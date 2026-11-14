select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(qtd_datasets_tratados_bd as integer) as qtd_datasets_tratados_bd,
    cast(qtd_tabelas_bigquery_municipio as integer) as qtd_tabelas_bigquery_municipio,
    cast(completude_chaves_ibge_pct as double) as completude_chaves_ibge_pct,
    cast(taxa_atualizacao_mensal_pct as double) as taxa_atualizacao_mensal_pct,
    cast(indice_cobertura_datalake_bd as double) as indice_cobertura_datalake_bd,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/basedosdados_bq/silver/bd_datalake_cobertura_municipio.parquet')
