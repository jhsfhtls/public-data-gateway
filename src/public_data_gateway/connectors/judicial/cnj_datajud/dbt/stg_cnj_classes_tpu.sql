{{ config(materialized='view') }}

select
    cast(classe_codigo_tpu as integer) as classe_codigo_tpu,
    trim(classe_nome) as classe_nome,
    upper(trim(natureza_processual)) as natureza_processual,
    upper(trim(ramo_aplicavel)) as ramo_aplicavel,
    trim(amparo_resolucao_cnj) as amparo_resolucao_cnj,
    extracted_at
from read_parquet('data/cnj_datajud/silver/cnj_classes_tpu.parquet')
