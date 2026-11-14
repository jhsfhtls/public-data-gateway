select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    dataset_bd_id,
    qtd_registros_municipio,
    completude_colunas_pct,
    latencia_atualizacao_dias,
    elegivel_join_federado_pct,
    reference_date
from {{ ref('stg_bd_datasets_tema') }}
