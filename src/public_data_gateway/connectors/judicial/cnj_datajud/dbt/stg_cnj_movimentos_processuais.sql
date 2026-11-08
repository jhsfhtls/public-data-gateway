{{ config(materialized='view') }}

select
    trim(id_movimento) as id_movimento,
    trim(numero_processo) as numero_processo,
    trim(data_hora_movimento) as data_hora_movimento,
    cast(movimento_codigo_tpu as integer) as movimento_codigo_tpu,
    trim(movimento_nome) as movimento_nome,
    trim(complemento_movimento) as complemento_movimento,
    cast(is_movimento_conclusivo as boolean) as is_movimento_conclusivo,
    extracted_at
from read_parquet('data/cnj_datajud/silver/cnj_movimentos_processuais.parquet')
