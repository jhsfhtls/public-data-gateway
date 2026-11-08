with raw_cad as (
    select * from {{ source('bcb_ifdata', 'bcb_ifdata_cadastro') }}
)

select
    cast(cnpj_14_lider as varchar) as cnpj_14_lider,
    cast(cod_inst as varchar) as cod_inst,
    cast(nome_instituicao as varchar) as nome_instituicao,
    cast(data_base as varchar) as data_base,
    cast(segmento_prudencial as varchar) as segmento_prudencial,
    cast(tipo_instituicao as varchar) as tipo_instituicao,
    cast(tcb as varchar) as tcb,
    cast(tc as integer) as tc,
    cast(atividade as varchar) as atividade,
    cast(uf_sede as varchar) as uf_sede,
    cast(municipio_sede as varchar) as municipio_sede,
    cast(cod_conglomerado_financeiro as varchar) as cod_conglomerado_financeiro,
    cast(cod_conglomerado_prudencial as varchar) as cod_conglomerado_prudencial,
    cast(situacao as varchar) as situacao
from raw_cad
