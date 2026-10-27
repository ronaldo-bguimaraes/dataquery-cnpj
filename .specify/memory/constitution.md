# dataquery-cnpj Constitution

## Sync Impact Report — 2026-10-27

- **Versão**: 1.0.0 → 1.1.0.
- **Alteração aprovada**: arquivos da camada Raw são preservados por competência no object
  storage para auditoria e reprocessamento; temporários locais continuam limitados à tarefa.
- **Motivo**: a regra anterior de remover dados brutos após uma execução conflitava com a
  arquitetura Medallion e com o requisito de reprocessar sem baixar novamente da RFB.
- **Impacto**: aumenta o uso de storage conforme as competências retidas; a política de
  expiração passa a ser decisão operacional explícita, não remoção automática após cada run.

Pipeline de dados abertos da Receita Federal (CNPJ) voltado a análise: extração dos arquivos
mensais publicados em WebDAV, conversão do CSV comprimido para Parquet e persistência em
armazenamento de objetos (S3/MinIO), formando a base sobre a qual a camada de consulta
(`dataquery`) será construída.

## Core Principles

### I. Estágios Desacoplados e Independentes

- Cada estágio do pipeline MUST ser um módulo Python com contrato explícito de entrada e
  saída, e MUST NOT depender de estado global, de ordem de importação ou de execução
  anterior em memória.
- Cada estágio MUST ser invocável por um entrypoint próprio com parâmetros explícitos.
  Caminhos, credenciais e competências MUST NOT ficar hardcoded no código; MUST vir de
  argumentos, de variáveis de ambiente ou de arquivo de configuração versionado.
- Novos estágios MUST ser adicionados sem alterar a lógica dos estágios existentes, exceto
  para corrigir um contrato violado.

Rationale: o pipeline hoje executa como efeito colateral do import e fixa
`/home/ronaldo/Documents/tmp/2026-02` e `comp = "2026-02"`, o que impede reprocessar outra
competência, rodar em CI e testar estágios de forma isolada.

### II. Contrato de Dados Explícito e Schema-First

- Todo conjunto de dados MUST ter schema declarado em um único registro central, com nomes
  de coluna e tipos, servindo de fonte única da verdade para extração, conversão e leitura.
- O conversor MUST consumir esse registro; listas de colunas MUST NOT ser duplicadas em
  outros módulos.
- Arquivo cujo nome não case com nenhum schema MUST falhar de imediato com erro explícito
  e nome do arquivo, MUST NOT ser processado com colunas adivinhadas.
- Alteração de schema MUST atualizar o registro e o README no mesmo commit, porque o Parquet
  gravado é o contrato consumido pelo consumidor final.

Rationale: `schema.py` já declara os dez conjuntos da RFB, mas `read_files.py` mantém uma
cópia divergente das colunas de `empresas`; a divergência silenciosa quebra o contrato do
Parquet gerado.

### III. Streaming e Uso Mensurável de Recursos

- Nenhum estágio MUST carregar um arquivo integralmente em memória. O processamento MUST
  ser streaming, do download ao Parquet.
- O conversor MUST usar a API de streaming do PyArrow (`open_csv` com `block_size` calculado
  sobre a memória disponível), que é o caminho validado em benchmark.
- `pandas.read_csv` MUST NOT ser usado no caminho de produção; permanece apenas como
  baseline de comparação de performance.
- `polars` MUST NOT ser introduzido antes de resolver o suporte a `latin1`, encoding
  padrão dos arquivos da RFB.
- Regressão de throughput superior a 20% frente ao baseline documentado no README MUST ser
  tratada como defeito, com medição reproduzível antes do merge.

Rationale: o baseline registrado no README é de 2 GB de CSV para 26.228.745 linhas, com
83,14s no PyArrow contra 162,00s no Pandas; essa diferença é o argumento do projeto.

### IV. Falhas Visíveis e Execução Observável

- Erros MUST NOT ser silenciados. `except` genérico que descarta a exceção MUST NOT existir
  sem log explícito do motivo e sem contabilização da falha.
- Toda etapa MUST reportar progresso observável: arquivos processados, linhas gravadas,
  duração e contagem de falhas.
- Falha MUST deixar o processo com status de erro distinto de sucesso. O código de saída
  MUST distinguir sucesso parcial de sucesso total.
- Reexecução MUST NOT deixar artefatos parciais: downloads MUST seguir o padrão de escrita
  em arquivo temporário seguido de rename atômico.

Rationale: hoje `_download_file` captura toda exceção com `except:` e descarta o traceback,
fazendo uma competência incompleta parecer um download bem-sucedido.

### V. Testes Primeiro (NON-NEGOTIABLE)

- Toda mudança de comportamento MUST começar por teste falhando, com aprovação do usuário
  antes da implementação, seguindo o ciclo Red-Green-Refactor.
- Testes unitários MUST cobrir: resolução de schema por regex, conversão CSV para Parquet
  (colunas, tipos, encoding `latin1`, delimitador `;`, contagem de linhas) e derivação do
  tamanho de bloco a partir da memória disponível.
- Testes de integração MUST cobrir: listagem WebDAV da RFB e round-trip de gravação e
  leitura no MinIO.
- O pipeline MUST ter ao menos um teste end-to-end com amostra reduzida de dados.
- A cobertura MUST ser medida e MUST NOT regredir entre versões.

Rationale: o repositório não possui suíte de testes; sem ela, qualquer mudança no schema ou
na lógica de conversão é um risco silencioso para quem consome os Parquet.

### VI. Simplicidade e Dependências Disciplinadas

- YAGNI MUST ser o critério: a solução mais simples que satisfaça o requisito vence. Abstração
  nova MUST ter um segundo uso real comprovado, nunca uso hipotético.
- Dependência nova MUST ter justificativa registrada na spec (necessidade, alternativa
  avaliada, licença) antes de entrar no `requirements.txt`.
- Dependência declarada MUST estar em uso. `pyspark` MUST ser removido ou vir acompanhado
  de spec que justifique o uso.
- API pública MUST ser tipada e MUST documentar entradas, saídas e erros possíveis.

Rationale: o projeto opera sobre dados de 2 GB com recurso de memória limitado; complexidade
sem ganho mensurável custa dinheiro em tempo de execução e em manutenção.

## Restrições Operacionais, Segurança e Performance

### Segurança e privacidade

- Credenciais MUST vir de variável de ambiente ou de arquivo `.env` ignorado pelo git.
  Segredo MUST NOT ser commitado, MUST NOT aparecer em log e MUST NOT entrar em imagem de
  container.
- O token WebDAV público da RFB MUST NOT ser tratado como credencial secreta, mas MUST NOT
  ser impresso em log de produção sem necessidade funcional.
- Os arquivos da RFB são públicos, porém o conjunto `socios` contém CPF, dado pessoal sob a
  LGPD. Esse Parquet MUST NOT ser publicado em bucket de acesso público e CPF MUST ser
  mascarado em log, em exceção e em saída de teste.

### Contrato externo da RFB

- Layout, encoding (`latin1`), delimitador (`;`) e o nome dos arquivos são contrato de
  terceiros e podem mudar sem aviso. A detecção MUST ocorrer por teste de schema, e MUST NOT
  por tentativa e erro em produção.
- A competência (`AAAA-MM`) MUST ser o identificador primário de uma execução. Reprocessar
  uma competência MUST ser idempotente: arquivo já presente MUST NOT ser rebaixado, e a
  saída MUST ser substituída de forma atômica.

### Infraestrutura

- Serviços locais MUST ser provisionados pelo `docker compose` versionado no repositório.
  Nenhuma etapa MUST depender de instalação manual fora do compose.
- O storage MUST usar apenas a API S3 (boto3), para que o mesmo código funcione em MinIO
  local e em object storage gerenciado.

### Performance e retenção

- O baseline do README é o piso de performance do projeto. Qualquer novo estágio MUST
  registrar seu custo e sua memória de pico.
- Os artefatos da camada Raw MUST ser preservados por competência para auditoria e
  reprocessamento. Sua expiração só pode ocorrer por uma política de retenção configurada e
  documentada; tarefas MUST remover seus temporários locais ao concluir ou falhar.

## Fluxo de Trabalho, Revisão e Gates de Qualidade

- O projeto segue desenvolvimento orientado por especificação com o Spec Kit. Toda feature
  MUST passar por `/speckit.specify`, `/speckit.clarify`, `/speckit.plan`, `/speckit.tasks`,
  `/speckit.implement`, `/speckit.analyze` e `/speckit.checklist`, com os artefatos de spec
  versionados em `specs/`.
- A implementação MUST seguir as tasks da spec. Mudança de escopo MUST passar por
  atualização da spec, e não por desvio silencioso no código.
- Gates obrigatórios antes de merge:
  - suíte de testes passando e cobertura sem regressão
  - conformidade com esta constituição verificada pelo revisor
  - README atualizado quando houver mudança de contrato, schema ou forma de uso
  - ausência de caminho hardcoded, segredo em código e `except` genérico
- Commits MUST seguir Conventional Commits (`feat`, `fix`, `refactor`, `docs`, `chore`),
  mantendo o histórico legível por etapa do pipeline.
- Todo pull request MUST listar as specs e os princípios tocados.

## Governance

- Esta constituição prevalece sobre qualquer prática padrão de agente ou ferramenta. Em
  conflito, prevalece o texto desta constituição.
- Emenda segue o fluxo: proposta motivada, decisão do maintainer, atualização do conteúdo
  com Sync Impact Report, incremento da versão e merge. Alteração que remova ou redefina
  um princípio exige MAJOR e plano de migração das features existentes.
- Política de versão (semver):
  - MAJOR: remoção ou redefinição de princípio, ou mudança incompatível de contrato de
    dados
  - MINOR: princípio ou seção nova, ou expansão material de orientação existente
  - PATCH: esclarecimento, correção de redação ou refino não semântico
- Revisão de conformidade: toda revisão MUST verificar a aderência aos princípios. Desvio
  MUST ser documentado na spec da feature e aprovado explicitamente; workarounds
  temporários MUST ter ticket e prazo de remoção.
- Divergência MUST ser corrigida antes da próxima feature do mesmo domínio.

**Version**: 1.1.0 | **Ratified**: 2026-10-22 | **Last Amended**: 2026-10-27
