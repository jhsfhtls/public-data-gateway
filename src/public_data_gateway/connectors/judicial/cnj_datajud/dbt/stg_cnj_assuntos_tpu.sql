{{ config(materialized='view') }}

select
    cast(assunto_codigo_tpu as integer) as assunto_codigo_tpu,
    trim(assunto_descricao) as assunto_descricao,
    upper(trim(ramo_direito)) as ramo_direito,
    cast(is_tema_repetitivo as boolean) as is_tema_repetitivo,
    cast(score_peso_risco as double) as score_peso_risco,
    extracted_at
from read_parquet('data/cnj_datajud/silver/cnj_assuntos_tpu.parquet')
