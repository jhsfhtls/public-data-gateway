with cad as (
    select * from {{ ref('stg_cvm_cadastro_fundos') }}
)

select
    cnpj_14,
    codigo_cvm,
    razao_social,
    tipo_fundo,
    classe_cvm,
    classe_anbima,
    situacao_registro,
    tipo_condominio,
    fundo_cotas,
    fundo_exclusivo,
    tributacao_longo_prazo,
    publico_alvo,
    cnpj_administrador,
    nome_administrador,
    cnpj_gestor,
    nome_gestor,
    cnpj_custodiante,
    nome_custodiante,
    nome_auditor,
    patrimonio_liquido,
    data_patrimonio_liquido,
    data_registro,
    data_constituicao,
    data_inicio_atividade,
    data_cancelamento
from cad
