# Requirements Checklist — Prospecção Comercial B2B

**Spec**: [../spec.md](../spec.md)

## Qualidade da especificação

- [x] Histórias priorizadas e testáveis de forma independente (US1, US2, US3).
- [x] Cada história tem justificativa, teste independente e cenários de aceite.
- [x] Requisitos funcionais numerados e verificáveis (FR-001 a FR-030).
- [x] Critérios de sucesso mensuráveis (SC-001 a SC-006).
- [x] Entidades-chave identificadas.
- [x] Casos de borda cobrem ausência de dados, contato, CPF, CSV e paginação.
- [x] Escopo explicitamente fora: intenção de compra, orçamento, necessidade.
- [x] Sem detalhe de implementação prescritivo na spec (stack fica no plan).

## Cobertura dos requisitos do produto

- [x] Granularidade por estabelecimento com CNPJ raiz (FR-001).
- [x] Identificação, localização, atividade, situação, perfil e tributação (FR-002–FR-004).
- [x] Contatos cadastrais com ressalva de origem (FR-003).
- [x] Indicadores derivados: idade, CNAEs, estabelecimentos, filiais, segmento (FR-005–FR-008).
- [x] Filtros por CNAE, localização, porte, situação e estrutura (FR-009–FR-014).
- [x] Matriz/filial identificáveis (FR-007, FR-013).
- [x] Geração de lista para prospecção (FR-019–FR-022).
- [x] Agrupamento por segmento e região (FR-018).
- [x] Não restringe a distribuidoras/fornecedores do governo (catálogo amplo).
- [x] Sem inferência de intenção comercial (FR-025).

## Prontidão para implementação

- [x] Constituição verificada no plan (10/10 princípios).
- [x] Modelo de dados com colunas, tipos, índices e regras de cálculo.
- [x] Estratégia de migração idempotente (`ALTER TABLE IF NOT EXISTS` + recarga).
- [x] Plano de testes unitário, integração e contrato.
- [x] Riscos de PII tratados (CPF fora do read model de listagem e do CSV).
