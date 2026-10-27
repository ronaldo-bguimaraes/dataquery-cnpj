# Contratos — Primeira DAG de Download de Dados CNPJ

**Feature**: `002-cnpj-download-dag`

| Contrato | Arquivo | Escopo |
|---|---|---|
| DAG e parâmetros | [dag-params.md](./dag-params.md) | Interface de acionamento manual: `conf` JSON, validação, pré/pós-condições, resumo e falhas |
| API chamável e CLI | [download-api.md](./download-api.md) | Funções públicas, tipos de resultado, exceções, garantias de idempotência/atomicidade e códigos de saída |

**Fontes de verdade**

- Regras de domínio e entidades: [../data-model.md](../data-model.md)
- Requisitos: [../spec.md](../spec.md)
- Decisões de arquitetura e evidências: [../research.md](../research.md)

**Regra de compatibilidade**: o `dag_id`, os nomes dos parâmetros e os campos do resumo são
contrato observável. Campos novos podem ser adicionados ao resumo; campos existentes não mudam
de tipo nem de significado sem atualização desta spec. Nomes de arquivo da RFB não são
renomeados.

**Segurança**: nenhum contrato expõe URL de download, token, credencial ou CPF. O resumo
contém apenas nomes de arquivo, status, contagens, bytes e duração.
