{{ config(materialized='view') }}

select
    trim(numero_processo) as numero_processo,
    upper(trim(tribunal_sigla)) as tribunal_sigla,
    upper(trim(grau_jurisdicao)) as grau_jurisdicao,
    cast(classe_codigo_tpu as integer) as classe_codigo_tpu,
    upper(trim(classe_nome)) as classe_nome,
    try_cast(data_ajuizamento as date) as data_ajuizamento,
    cast(valor_causa as double) as valor_causa,
    trim(orgao_julgador_nome) as orgao_julgador_nome,
    trim(orgao_julgador_municipio_ibge) as orgao_julgador_municipio_ibge,
    upper(trim(sistema_origem)) as sistema_origem,
    cast(is_segredo_justica as boolean) as is_segredo_justica,
    upper(trim(situacao_processual)) as situacao_processual,
    upper(trim(polo_ativo_tipo)) as polo_ativo_tipo,
    trim(polo_passivo_documento) as polo_passivo_documento,
    upper(trim(polo_passivo_nome)) as polo_passivo_nome,
    extracted_at
from read_parquet('data/cnj_datajud/silver/cnj_processos.parquet')
