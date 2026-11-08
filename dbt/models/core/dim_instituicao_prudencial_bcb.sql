with cad as (
    select * from {{ ref('stg_bcb_ifdata_cadastro') }}
)

select
    cnpj_14_lider,
    cod_inst,
    nome_instituicao,
    data_base,
    segmento_prudencial,
    tipo_instituicao,
    tcb,
    tc,
    atividade,
    uf_sede,
    municipio_sede,
    cod_conglomerado_financeiro,
    cod_conglomerado_prudencial,
    situacao
from cad
