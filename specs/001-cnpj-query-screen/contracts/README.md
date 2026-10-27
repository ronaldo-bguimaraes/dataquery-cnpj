# Contratos — Tela de Consulta de Dados CNPJ

**Feature**: `001-cnpj-query-screen`

| Contrato | Arquivo | Escopo |
|---|---|---|
| API de consulta | [query-api.md](./query-api.md) | REST v1 do serviço Python (DuckDB): ficha por CNPJ, listagem filtrada, busca fuzzy, lookups, saúde |
| Artefatos de dados | [data-contracts.md](./data-contracts.md) | Layout físico, nomenclatura e regras de compatibilidade dos Parquet no bucket |

**Fontes de verdade**
- Nomes e tipos de coluna: `schema.py` (Princípio II da constituição)
- Regras de domínio e validação: [../data-model.md](../data-model.md)
- Decisões de arquitetura: [../research.md](../research.md)

**Regra de compatibilidade**: enquanto a v1 da API estiver publicada, novos campos podem ser
adicionados às respostas, mas campos existentes não mudam de tipo nem de significado. Remoções
exigem v2. Para os artefatos Parquet, mudança de layout exige reindexação e incrementação da
versão do artefato.
