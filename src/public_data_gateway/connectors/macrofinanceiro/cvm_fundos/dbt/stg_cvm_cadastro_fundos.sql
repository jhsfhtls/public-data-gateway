with raw_cad as (
    select * from {{ source('cvm_fundos', 'cvm_cadastro_fundos') }}
)

select
    cast(cnpj_14 as varchar) as cnpj_14,
    cast(codigo_cvm as varchar) as codigo_cvm,
    cast(razao_social as varchar) as razao_social,
    cast(tipo_fundo as varchar) as tipo_fundo,
    cast(classe_cvm as varchar) as classe_cvm,
    cast(classe_anbima as varchar) as classe_anbima,
    cast(situacao_registro as varchar) as situacao_registro,
    cast(tipo_condominio as varchar) as tipo_condominio,
    cast(fundo_cotas as varchar) as fundo_cotas,
    cast(fundo_exclusivo as varchar) as fundo_exclusivo,
    cast(tributacao_longo_prazo as varchar) as tributacao_longo_prazo,
    cast(publico_alvo as varchar) as publico_alvo,
    cast(cnpj_administrador as varchar) as cnpj_administrador,
    cast(nome_administrador as varchar) as nome_administrador,
    cast(cnpj_gestor as varchar) as cnpj_gestor,
    cast(nome_gestor as varchar) as nome_gestor,
    cast(cnpj_custodiante as varchar) as cnpj_custodiante,
    cast(nome_custodiante as varchar) as nome_custodiante,
    cast(nome_auditor as varchar) as nome_auditor,
    cast(patrimonio_liquido as double) as patrimonio_liquido,
    cast(data_patrimonio_liquido as date) as data_patrimonio_liquido,
    cast(data_registro as date) as data_registro,
    cast(data_constituicao as date) as data_constituicao,
    cast(data_inicio_atividade as date) as data_inicio_atividade,
    cast(data_cancelamento as date) as data_cancelamento
from raw_cad
