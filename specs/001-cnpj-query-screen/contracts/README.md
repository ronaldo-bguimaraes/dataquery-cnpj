# Contratos — Explorador Visual de Dados CNPJ

**Feature**: `001-cnpj-query-screen`

| Contrato | Arquivo | Escopo |
|---|---|---|
| HTTP das páginas | [query-api.md](./query-api.md) | Rotas HTML, parâmetros de filtro, estados, erros e regras de privacidade. Não há API JSON pública no MVP. |
| Dados | [data-contracts.md](./data-contracts.md) | Parquets de origem, transformações de carga, schema PostgreSQL, índices e compatibilidade. |

**Fontes de verdade**
- Nomes de coluna da origem: `schema.py` (Princípio II da constituição)
- Regras de domínio e validações: [../data-model.md](../data-model.md)
- Decisões de arquitetura: [../research.md](../research.md)
- Especificação: [../spec.md](../spec.md)

**Regra de compatibilidade**: a competência é imutável. Reexecutar a carga da mesma
competência substitui o retrato de forma atômica; nenhuma versão parcial fica visível.
Mudanças no schema de consulta exigem atualização deste contrato e da versão registrada em
`query_metadata`. O MVP não oferece API pública, exportação ou download.

**Consumidores futuros**: uma API para terceiros e ferramentas de BI (como Superset) poderão
usar o PostgreSQL somente leitura e o read model estável (`estabelecimentos_listagem`).
Esses consumidores terão contratos próprios e não fazem parte do MVP; a API exigirá
autenticação/rate limiting e o BI deverá usar banco de metadados próprio e, em produção,
réplica de leitura/base analítica.
