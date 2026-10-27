# Contratos — DAG Raw → Bronze

**Feature**: `002-cnpj-download-dag`

| Contrato | Arquivo | Escopo |
|---|---|---|
| DAG, parâmetros e mapping | [dag-params.md](./dag-params.md) | Trigger manual, listagem, seleção e tarefas por ZIP |
| Funções de domínio | [download-api.md](./download-api.md) | Download unitário, reuso Raw, conversão Parquet |
| Armazenamento | [storage.md](./storage.md) | Protocolo genérico, fábrica e backends filesystem/S3 |

Fontes: [spec](../spec.md), [modelo](../data-model.md), [pesquisa](../research.md).

XCom contém nomes, chaves **lógicas**, estados e contagens. Nunca conteúdo de arquivo,
URL com token, credenciais ou path absoluto do worker.
