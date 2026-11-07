# Cobertura dos dados e melhorias futuras

Este documento registra o que a competência `2026-10` dos Parquets atuais cobre e
quais evoluções podem transformar o pipeline em um portal de exploração interativa.
É um inventário e uma direção de produto, não um compromisso de implementação ou
uma definição final de público-alvo.

## Decisão de arquitetura: PostgreSQL

PostgreSQL fica definido como o banco relacional local de desenvolvimento e como a
camada consultável do portal. A API Spring Boot e ferramentas de análise como Apache
Superset poderão consultar tabelas e views desse banco.

Os Parquets por competência continuam preservados como snapshots da fonte e artefatos
reprocessáveis. A decisão não implica carregar todo o histórico bruto no PostgreSQL:
o banco receberá os dados relacionais preparados para as consultas e funcionalidades
priorizadas. O escopo inicial da carga — por exemplo, apenas a competência mais
recente — será definido na implementação.

PostgreSQL é a escolha atual para desenvolvimento local e primeira versão; não há
decisão de usar Amazon Redshift. Uma migração ou adoção de warehouse só será avaliada
se o volume e o padrão real de consultas justificarem essa complexidade.

## Cobertura atual

Os dados são os arquivos públicos do Cadastro Nacional da Pessoa Jurídica publicados
pela Receita Federal, convertidos para Parquet por competência.

| Área | Dados disponíveis nos Parquets | Observações |
|---|---|---|
| Empresas | CNPJ básico, razão social, natureza jurídica, qualificação do responsável, capital social, porte e ente federativo | Relaciona-se com estabelecimentos, sócios e Simples pelo CNPJ básico. |
| Estabelecimentos | CNPJ completo derivável, matriz/filial, nome fantasia, situação e data da situação cadastral, endereço, município, UF, CNAE principal e secundários | Os telefones e e-mail são os informados no cadastro da Receita; podem estar ausentes ou desatualizados e não são enriquecidos/verificados pelo projeto. |
| Sócios (QSA) | Nome, identificador, CPF/CNPJ conforme arquivo-fonte, qualificação, data de entrada, país, representante legal e faixa etária | CPF/CNPJ de pessoa física exige tratamento de privacidade e não deve ser exposto integralmente na interface ou em logs. |
| Simples Nacional e MEI | Indicadores de opção, datas de opção e datas de exclusão | Refletem o arquivo da competência carregada. As datas nos campos não substituem uma série de snapshots mensais. |
| Tabelas de apoio | CNAEs, municípios, naturezas jurídicas, motivos, países e qualificações | Usadas para apresentar descrições legíveis para os códigos. |

Com esses dados, o MVP pode oferecer busca e filtros cadastrais/geográficos, ficha de
empresa e estabelecimentos, recortes de Simples/MEI, tabela de resultados, contagens
e gráficos derivados dos filtros. O MVP é somente para visualização e filtragem: não
inclui exportação nem download de arquivos.

### Limite de cobertura temporal

Os Parquets estão organizados por competência, mas o inventário local verificado
contém `2026-10`. Uma única competência permite consultar o retrato daquele mês; não
permite comparar mudanças entre meses. Para oferecer histórico, o pipeline precisa
reter mais competências e a camada de consulta precisa comparar os snapshots. A
presença de uma data de opção, exclusão ou situação cadastral no registro não equivale
a um histórico completo de versões do cadastro.

## Melhorias futuras

As ideias abaixo ampliam a exploração dos dados atuais ou incorporam fontes novas.
Devem ser priorizadas conforme as perguntas reais dos usuários.

### MVP — visualização e filtragem

- Compor filtros combináveis por competência, situação, UF, município, CNAE, porte,
  matriz/filial, abertura e Simples/MEI.
- Atualizar contagem, prévia tabular e gráficos quando os filtros mudarem.
- Permitir ordenar e paginar os resultados sem perder os filtros.
- Exibir fonte e competência junto aos resultados.
- Não oferecer exportação ou download no MVP.

### Melhorias posteriores de exploração

- Exportar recortes filtrados em CSV.
- Salvar e comparar consultas e criar dashboards pessoais.

### Histórico por competência

- Reter snapshots mensais e deixar o usuário escolher a competência disponível.
- Comparar competências para identificar aberturas, baixas e alterações de endereço,
  situação, CNAE ou opção tributária.
- Avaliar uma tabela de mudanças ou modelo SCD somente quando houver consultas
  históricas concretas que justifiquem materializar esse histórico no banco relacional.
- Não assumir que todos os registros tenham uma chave estável para acompanhar mudanças;
  tratar QSA com cuidado especial.

### Fontes externas ainda não integradas

| Possível módulo | Situação atual | Trabalho necessário |
|---|---|---|
| Lucro Real, Presumido e Arbitrado | Não consta nos Parquets CNPJ atuais | Localizar fonte pública adequada, confirmar cobertura e competência, licenças/termos e modelar sua integração. |
| Dívida ativa da União (PGFN) | Não consta nos Parquets CNPJ atuais | Integrar fonte da PGFN, documentar granularidade e significado dos valores, e relacionar registros de forma auditável. |
| Cadastro Nacional de Obras (CNO) | Não consta nos Parquets CNPJ atuais | Localizar e integrar os dados de CNO e definir relacionamento com responsáveis e empresas. |
| Telefones/e-mails enriquecidos | O projeto tem apenas os campos presentes nos dados cadastrais da Receita | Uma camada enriquecida exigiria fonte/provedor separado, proveniência, data de verificação, qualidade e revisão de privacidade. |

Não apresentar esses módulos como disponíveis até que as fontes tenham sido obtidas,
validadas e integradas. Dados de fontes distintas devem manter sua própria origem,
competência e regras de atualização.

## Ingestão de próxima geração

A feature 003 estabelece a ingestão paralela e confiável para a competência completa
(≈223 milhões de registros): loading em 8 workers com partições lógicas (64–128),
transação atômica na camada Iceberg Silver, staging UNLOGGED + swap atômico no
serving, idempotência por `load_id`, ledger de rejeições e rollback. Detalhes:

- [Spec](../specs/003-reliable-ingestion/spec.md)
- [Plan](../specs/003-reliable-ingestion/plan.md)
- [Data model](../specs/003-reliable-ingestion/data-model.md)
- [Quickstart](../specs/003-reliable-ingestion/quickstart.md)
- [Runbook de operação](../airflow/dags/runbook.md)

Cobertura da Constitution v2.0.0: ACID, idempotência, deduplicação, incremental,
tolerância a falha, batch atômico, execução distribuída, consumo limitado,
observabilidade e versionamento/reprodutibilidade (10/10 do check).

## Direção de produto

A hipótese atual é oferecer um explorador visual dos dados públicos: a pessoa escolhe
uma competência, monta um recorte com filtros e entende os resultados em tabela e
gráficos. O MVP não exporta arquivos. A proposta é permitir que o usuário explore os
dados diretamente em vez de depender apenas de planilhas pré-segmentadas entregues
pelo projeto.

Isso ainda não define um público-alvo. O portal deve começar com os dados CNPJ já
disponíveis e com fluxos simples de exploração; integrações tributárias, IA, API
pública, autenticação, cobrança e enriquecimento são possibilidades posteriores, não
requisitos assumidos para a primeira versão.

## Referências internas

- [Pipeline e armazenamento](../README.md#pipeline-cnpj-raw--bronze)
- [Spec da tela de consulta](../specs/001-cnpj-query-screen/spec.md)
- [Modelo de dados da tela](../specs/001-cnpj-query-screen/data-model.md)
