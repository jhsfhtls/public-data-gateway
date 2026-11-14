select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(dataset_bd_id as varchar) as dataset_bd_id,
    cast(qtd_registros_municipio as integer) as qtd_registros_municipio,
    cast(completude_colunas_pct as double) as completude_colunas_pct,
    cast(latencia_atualizacao_dias as integer) as latencia_atualizacao_dias,
    cast(elegivel_join_federado_pct as double) as elegivel_join_federado_pct,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/basedosdados_bq/silver/bd_datasets_publicos_tema.parquet')
