select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(dicionario_padronizado_pct as double) as dicionario_padronizado_pct,
    cast(conformidade_chaves_canonicas_pct as double) as conformidade_chaves_canonicas_pct,
    cast(disponibilidade_bq_sql_pct as double) as disponibilidade_bq_sql_pct,
    cast(indice_governanca_bd as double) as indice_governanca_bd,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/basedosdados_bq/silver/bd_governanca_metadados_odcs.parquet')
