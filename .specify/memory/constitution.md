# dataquery-cnpj Constitution

Este documento estabelece as leis de engenharia do projeto **dataquery-cnpj**, que ingere,
processa e serve dados públicos de CNPJ da Receita Federal. Os princípios são
technology-agnostic e se aplicam a todos os componentes presentes e futuros, incluindo
ingestão, transformação, armazenamento, orquestração, serving e APIs de consulta.

## Core Principles

### 1. ACID Transactions

- Toda modificação persistente MUST ter limites transacionais explícitos.
- Operações com múltiplas etapas MUST confirmar ou reverter de forma atômica quando o
  armazenamento suportar transações; escrita parcial MUST NOT ser reportada como sucesso.
- Dados e metadados de conclusão de uma operação MUST ser confirmados dentro da mesma
  fronteira transacional sempre que possível.
- Quando o armazenamento não oferecer ACID, a implementação MUST usar um protocolo
  equivalente de publicação idempotente e compensação; a exceção MUST ser justificada e
  documentada.

Rationale: uma publicação parcial contamina todos os consumidores e torna o reprocessamento
não confiável.

### 2. Idempotency

- Ingestões, execuções em lote e tarefas distribuídas MUST tolerar execução repetida.
- Repetir uma operação MUST NOT criar registros duplicados nem efeitos colaterais não
  intencionais.
- A identidade de uma operação MUST ser determinística; retries MUST usar a mesma identidade
  lógica.
- Cada tarefa MUST ser segura para reexecução, com semântica de substituição, upsert ou
  descarte explicitamente documentada.

Rationale: falhas transientes, retries de orquestração e reprocessamentos são esperados; a
idempotência é a diferença entre recuperação e duplicação.

### 3. Data Deduplication and Integrity

- Todo conjunto de dados MUST definir sua identidade de registro e suas restrições de
  unicidade de forma explícita.
- Deduplicação MUST usar chaves de negócio estáveis sempre que existirem.
- Hashes de conteúdo MAY ser usados para detectar mudanças e otimizar processamento, mas
  MUST NOT substituir a identidade de negócio.
- Restrições de chave, relacionamento e domínio MUST ser verificadas na fronteira de
  ingestão e no armazenamento; registros rejeitados MUST ser contabilizados e observáveis.
- Schema e relacionamentos MUST ser explícitos; colunas MUST NOT ser inferidas por tentativa
  e erro em produção.

Rationale: sem identidade e restrições explícitas, duplicatas e órfãos passam silenciosamente
para as camadas seguintes.

### 4. Incremental Processing

- A ingestão MUST evitar reescritas completas desnecessárias.
- Quando a semântica da fonte permitir, o sistema MUST identificar registros novos,
  modificados, inalterados e removidos.
- Mudanças MUST ser aplicadas de forma incremental sempre que for prático.
- Uma carga completa MUST ser uma decisão explícita, justificada e documentada, nunca o
  caminho padrão por conveniência.
- A lógica incremental MUST ser determinística e coberta por testes com dados novos,
  alterados, inalterados e removidos.

Rationale: processamento incremental reduz custo, latência e janelas de indisponibilidade,
desde que a classificação de mudanças seja confiável.

### 5. Fault Tolerance and Recoverability

- Jobs de longa duração MUST suportar checkpoints, retries, recuperação e retomada segura.
- Trabalho já concluído MUST NOT ser repetido desnecessariamente após uma falha.
- Falhas MUST deixar estado explícito; artefatos parciais MUST ser isolados, removidos ou
  claramente identificados como não publicados.
- Retries MUST ser idempotentes e MUST NOT mascarar uma falha permanente.
- O caminho de recuperação MUST ser testado, não apenas documentado.

Rationale: tolerância a falhas sem recuperação verificável é apenas retry; o sistema precisa
provar que retoma sem corromper ou duplicar dados.

### 6. Atomic Batch Processing

- Cada lote processado de forma independente MUST ter uma fronteira de commit bem definida.
- Persistência dos dados e metadados de conclusão do lote MUST ser coordenadas; um lote MUST
  NOT ser marcado como concluído sem que seus dados estejam duramente confirmados.
- O manifesto do lote MUST registrar identidade da fonte, versão, contagens, rejeições,
  status e erros relevantes.
- Um lote com falha MUST NOT ser visível como concluído nem servir como base para etapas
  seguintes.

Rationale: metadados de sucesso e dados precisam contar a mesma história; caso contrário, a
orquestração avança sobre dados incompletos.

### 7. Distributed Execution

- O trabalho MUST ser decomponível em tarefas independentes que possam executar
  concorrentemente em múltiplos workers.
- Propriedade de tarefa, retries, conflitos de concorrência e falhas de worker MUST ser
  tratados explicitamente.
- Tarefas MUST NOT pressupor memória compartilhada ou ordem global de execução.
- A partição de trabalho MUST ser determinística e cada partição MUST ter um único
  responsável por vez.
- O estado da orquestração e o commit dos dados MUST ser coordenados; uma tarefa só pode ser
  considerada concluída quando seu efeito durável estiver confirmado.

Rationale: paralelismo sem ownership e coordenação explícitos troca lentidão por corrupção
e resultados não determinísticos.

### 8. Bounded Resource Consumption

- Nenhum processamento MUST exigir carregar um conjunto de dados grande inteiro em memória.
- Streaming, chunking, particionamento, backpressure e processamento com apoio em disco MUST
  ser preferidos para grandes volumes.
- Limites de memória, disco, conexões e concorrência MUST ser explícitos e monitorados.
- Buffers, filas e caches MUST ser limitados; crescimento sem limite MUST ser tratado como
  defeito.
- Quando o recurso atingir o limite, a execução MUST falhar de forma observável, não provocar
  degradação silenciosa.

Rationale: consumo ilimitado transforma um pico de volume em indisponibilidade e perda de
trabalho.

### 9. Observability and Auditability

- Todo job de ingestão MUST expor status de execução, identidade da fonte, contagens de
  registros processados, rejeitados e retidos, retries, falhas e histórico de execução.
- Operações MUST ser rastreáveis até a fonte, o job, a versão do código e a configuração que
  as produziu.
- Logs MUST ser estruturados, correlacionáveis e livres de segredos e dados pessoais; CPF
  MUST ser mascarado em log, exceção e saída de teste.
- Registros de auditoria MUST ter retenção definida e MUST NOT ser alterados silenciosamente.
- Métricas de duração, volume, rejeição, retry e recuperação MUST ser registradas para cada
  estágio relevante.

Rationale: sem trilha auditável, não é possível distinguir sucesso, falha parcial e
reprocessamento.

### 10. Data Versioning and Reproducibility

- Datasets de origem MUST ser identificáveis por versão ou snapshot, incluindo competência,
  data de referência e origem.
- Resultados de processamento MUST ser rastreáveis até a versão exata da fonte e a lógica de
  transformação que os produziu.
- Dados brutos MUST ser imutáveis e preservados conforme a política de retenção; camadas
  derivadas MUST ser reproduzíveis a partir deles.
- Metadados de versão MUST ser atualizados na mesma operação durável dos dados que descrevem.
- Quando o armazenamento suportar snapshots ou time travel, eles MUST ser usados para
  auditoria e reprocessamento; quando não suportar, manifests e hashes MUST registrar a
  versão lógica.

Rationale: reprodutibilidade é o que permite investigar divergências, refazer resultados e
comparar competências sem depender de estado implícito.

## Restrições Operacionais e de Dados

### Fonte e contrato externo

- A fonte é o cadastro público de CNPJ da Receita Federal, organizado por competência
  (`AAAA-MM`). Layout, encoding, delimitadores e nomes de arquivos são contrato de terceiros
  e podem mudar sem aviso.
- Mudanças na fonte MUST ser detectadas por validação explícita de schema e contrato, nunca
  por tentativa e erro em produção.
- A competência MUST ser o identificador primário de uma execução e MUST acompanhar todos os
  artefatos e resultados.

### Contrato de dados e compatibilidade

- Cada conjunto de dados MUST ter schema declarado em um único registro central, com nomes,
  tipos e restrições, servindo de fonte única da verdade.
- Listas de colunas e regras de schema MUST NOT ser duplicadas em módulos consumidores.
- Arquivo ou registro que não corresponda ao contrato MUST falhar de forma explícita, com
  identificação da origem.
- Mudanças de schema, identidade ou semântica MUST atualizar o contrato, a documentação e os
  testes no mesmo commit; compatibilidade retroativa MUST ser avaliada.

### Segurança, privacidade e retenção

- Credenciais MUST vir de variáveis de ambiente ou arquivo de configuração ignorado pelo
  versionamento; segredos MUST NOT ser commitados, logados ou embutidos em imagens.
- O conjunto de sócios contém CPF, dado pessoal sob a LGPD; CPF MUST ser mascarado em qualquer
  exibição, log, exceção ou saída de teste, e os dados MUST NOT ser publicados em storage de
  acesso público.
- Acesso a dados e serviços MUST seguir o princípio do menor privilégio.
- Artefatos brutos MUST ser preservados por competência para auditoria e reprocessamento;
  expiração MUST ocorrer apenas por política de retenção explícita e documentada.
- Temporários locais e artefatos parciais MUST ser removidos ao concluir ou falhar.

### Performance e infraestrutura

- Todo estágio novo MUST registrar duração, volume processado, memória de pico e custo
  operacional relevante.
- Regressão de throughput superior a 20% frente ao baseline documentado MUST ser tratada como
  defeito, com medição reproduzível antes do merge.
- Serviços e ambientes locais MUST ser reproduzíveis por configuração versionada; nenhuma
  etapa MUST depender de instalação ou ajuste manual fora do fluxo documentado.
- Acesso a armazenamento MUST passar pela abstração do projeto, sem acoplamento do domínio a
  um provedor específico.

### Tecnologia e dependências

- Preferir padrões e bibliotecas comprovadas a implementações próprias de primitivas de
  sistemas distribuídos, transações, retries ou deduplicação.
- Nova infraestrutura ou dependência MUST ter necessidade demonstrada, alternativas
  avaliadas, licença verificada e justificativa registrada antes de ser adotada.
- Complexidade arquitetural MUST NOT ser introduzida sem um requisito real e mensurável.
- Dependência declarada MUST estar em uso e MUST ser removida quando não for mais necessária.

## Fluxo de Trabalho, Revisão e Gates de Qualidade

- O projeto segue desenvolvimento orientado por especificação com o Spec Kit. Toda feature
  MUST passar por `/speckit.specify`, `/speckit.clarify`, `/speckit.plan`, `/speckit.tasks`,
  `/speckit.implement`, `/speckit.analyze` e `/speckit.checklist`, com os artefatos
  versionados em `specs/`.
- A implementação MUST seguir as tasks da spec. Mudança de escopo MUST atualizar a spec, não
  ser absorvida por desvio silencioso no código.
- Testes primeiro (TDD) é NON-NEGOTIABLE: toda mudança de comportamento MUST começar por
  teste falhando e seguir Red-Green-Refactor.
- A suíte MUST cobrir, conforme aplicável: contratos, idempotência, atomicidade, deduplicação,
  concorrência, retries, recuperação, versionamento e limites de recurso.
- Testes de integração e end-to-end MUST usar amostras representativas e MUST NOT depender de
  estado produzido manualmente.
- A cobertura MUST ser medida e MUST NOT regredir entre versões.
- Gates obrigatórios antes de merge:
  - suíte passando e cobertura sem regressão;
  - conformidade com esta constituição verificada pelo revisor;
  - testes das garantias críticas citadas acima;
  - documentação atualizada quando houver mudança de contrato, schema, operação ou uso;
  - ausência de segredo, caminho hardcoded, `except` genérico sem tratamento e artefato
    parcial publicado como sucesso.
- Pull requests MUST listar as specs e os princípios afetados.
- Exceções arquiteturais MUST ser explícitas, justificadas, aprovadas e documentadas, com
  responsável e prazo de revisão.
- Commits MUST seguir Conventional Commits (`feat`, `fix`, `refactor`, `docs`, `chore`).

## Governance

- Esta constituição prevalece sobre práticas padrão de agentes, ferramentas e convenções
  locais. Em conflito, prevalece o texto desta constituição.
- Emenda segue o fluxo: proposta motivada, decisão do maintainer, atualização do conteúdo com
  Sync Impact Report, incremento de versão e merge.
- Política de versão (semver):
  - MAJOR: remoção ou redefinição incompatível de princípio, ou mudança incompatível de
    contrato de dados;
  - MINOR: princípio ou seção nova, ou expansão material de orientação existente;
  - PATCH: esclarecimento, correção de redação ou refino não semântico.
- Toda revisão MUST verificar aderência aos princípios e às restrições operacionais.
- Desvios MUST ser documentados na spec ou em registro de decisão, aprovados explicitamente e
  corrigidos antes da próxima feature do mesmo domínio.
- Exceções temporárias MUST ter ticket, responsável e prazo de remoção.
- A constituição se aplica a todos os componentes e features futuras; nenhum componente pode
  declarar-se fora de seu escopo sem uma emenda ou exceção aprovada.

**Version**: 2.0.0 | **Ratified**: 2026-10-22 | **Last Amended**: 2026-11-06
