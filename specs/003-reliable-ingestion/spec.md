# Feature Specification: Ingestão Confiável e Escalável de Dados CNPJ

**Feature Branch**: `003-reliable-ingestion`

**Created**: 2026-11-06

**Status**: Draft

**Input**: User description: "Documentar as novas expectativas de ingestão: ACID, idempotência, deduplicação, processamento incremental, tolerância a falhas, batches atômicos, execução distribuída, recursos limitados, observabilidade e versionamento/reprodutibilidade."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Publicação atômica e íntegra de uma competência (Priority: P1)

Como pessoa que consome os dados, quero que uma competência publicada esteja sempre completa e consistente, para que nenhuma análise use uma carga parcial ou misturada.

**Why this priority**: É a garantia mais básica do pipeline. Sem publicação atômica e íntegra, as camadas seguintes não podem confiar nos dados, independentemente de velocidade ou paralelismo.

**Independent Test**: Executar a carga de uma competência e verificar que os consumidores veem somente a versão anterior completa ou a nova versão completa, com contagens e metadados coerentes, mesmo simulando falha no meio do processo.

**Acceptance Scenarios**:

1. **Given** uma competência anterior publicada e uma nova carga em andamento, **When** os consumidores consultam os dados, **Then** eles continuam vendo a versão anterior completa até a publicação da nova versão.
2. **Given** todos os lotes da nova competência foram processados e validados, **When** a publicação ocorre, **Then** dados e metadados de conclusão ficam disponíveis na mesma operação durável.
3. **Given** uma falha em qualquer lote antes da publicação, **When** o processo termina, **Then** a versão anterior permanece ativa, o lote não é marcado como concluído e nenhum dado parcial fica visível.
4. **Given** a mesma carga executada novamente, **When** o processo termina, **Then** as contagens e os registros publicados são os mesmos, sem duplicatas nem efeitos colaterais adicionais.
5. **Given** registros inválidos, duplicados ou fora do contrato, **When** a validação ocorre, **Then** eles são rejeitados, contabilizados e não impedem silenciosamente a publicação nem aparecem como sucesso.

---

### User Story 2 - Reprocessamento e retomada sem retrabalho (Priority: P2)

Como operador do pipeline, quero retomar uma carga interrompida sem repetir todo o trabalho já concluído, para reduzir custo e tempo de recuperação.

**Why this priority**: Falhas de worker, rede e armazenamento são esperadas em cargas longas. Recuperação segura é o que torna a operação viável.

**Independent Test**: Interromper uma carga no meio de uma partição, reiniciar o processo e verificar que as partições concluídas não são reprocessadas e que os dados finais não contêm duplicatas.

**Acceptance Scenarios**:

1. **Given** uma carga interrompida no meio de uma partição, **When** o operador reinicia a execução, **Then** o sistema retoma a partir do último checkpoint durável.
2. **Given** partições já concluídas antes da falha, **When** a execução é retomada, **Then** elas não são reprocessadas desnecessariamente.
3. **Given** um retry da mesma unidade lógica de trabalho, **When** ele termina, **Then** os registros não são duplicados e o estado final é idêntico ao de uma execução única bem-sucedida.
4. **Given** um worker que falha durante a escrita, **When** outro worker assume a tarefa, **Then** a propriedade é transferida explicitamente e não há duas escritas concorrentes para a mesma partição.
5. **Given** uma falha permanente, **When** o operador consulta o histórico, **Then** o status, a causa sanitizada e as partições afetadas ficam disponíveis para diagnóstico e nova tentativa.

---

### User Story 3 - Carga paralela e escalável dentro da janela operacional (Priority: P3)

Como operador da plataforma, quero processar a competência completa com múltiplos workers, para reduzir o tempo de carga sem comprometer integridade, memória ou estabilidade do serving.

**Why this priority**: O volume atual já é da ordem de 223 milhões de registros; escala é necessária para a carga deixar de ser uma operação manual longa.

**Independent Test**: Processar a mesma competência com um worker e com múltiplos workers, comparar duração, uso de memória e integridade dos resultados, e interromper workers durante a execução para validar recuperação.

**Acceptance Scenarios**:

1. **Given** uma competência dividida em partições independentes, **When** a carga inicia, **Then** as partições podem ser processadas concorrentemente sem memória compartilhada.
2. **Given** uma configuração de concorrência, **When** a carga executa, **Then** o número de tarefas simultâneas respeita o limite configurado.
3. **Given** um worker interrompido no meio da carga, **When** a execução continua, **Then** as outras partições continuam e a partição afetada pode ser retomada com segurança.
4. **Given** dois operadores disparam a mesma competência ao mesmo tempo, **When** o sistema detecta a corrida, **Then** apenas uma execução publica a competência e a outra é rejeitada, aguardada ou coordenada explicitamente.
5. **Given** recursos de memória, disco ou conexões limitados, **When** o limite é atingido, **Then** a execução falha de forma observável em vez de degradar silenciosamente ou esgotar o ambiente.
6. **Given** a competência completa, **When** a carga roda com múltiplos workers no mesmo ambiente, **Then** o tempo total é materialmente menor que o baseline de um worker, sem perda de integridade.

---

### User Story 4 - Rastreabilidade, versionamento e evolução incremental (Priority: P4)

Como responsável pelos dados, quero rastrear cada resultado até a versão exata da fonte e da lógica que o produziu, e aplicar mudanças de forma incremental, para auditar, reproduzir e evoluir o pipeline sem reescrever tudo.

**Why this priority**: Dados públicos mensais mudam e precisam ser auditáveis. Versionamento e incremental são o que permitem confiança de longo prazo e custo sustentável.

**Independent Test**: Executar duas competências ou duas versões da mesma competência, verificar que cada resultado aponta para a fonte e a versão de transformação corretas, que registros inalterados não são reescritos desnecessariamente e que a versão anterior continua acessível.

**Acceptance Scenarios**:

1. **Given** uma competência publicada, **When** o operador consulta seus metadados, **Then** encontra identidade da fonte, versão/snapshot, hashes dos arquivos, versão da lógica de transformação e contagens.
2. **Given** uma nova versão da mesma competência, **When** o processamento ocorre, **Then** a nova versão é identificável e a anterior continua disponível para auditoria e rollback.
3. **Given** uma fonte com registros novos, modificados, inalterados e removidos, **When** a semântica permitir classificá-los, **Then** o sistema registra a classificação e aplica apenas as mudanças necessárias.
4. **Given** uma execução sem mudanças relevantes, **When** a fonte é comparada, **Then** o sistema evita reescrever o conjunto inteiro e registra que não houve alteração.
5. **Given** a necessidade de uma carga completa, **When** ela é executada, **Then** a decisão é explícita, justificada e registrada no histórico.
6. **Given** uma evolução de schema compatível, **When** novos campos são incorporados, **Then** a versão anterior continua legível e o contrato de dados é atualizado.
7. **Given** registros rejeitados, **When** a auditoria é consultada, **Then** motivo, contagem e origem estão disponíveis sem expor CPF ou conteúdo sensível.

---

### Edge Cases

- Arquivo de origem ausente, duplicado, vazio ou com hash diferente do manifesto: a carga falha antes de publicar e o motivo fica registrado.
- Layout, encoding, delimitador ou nome de arquivo da fonte muda: a validação de contrato falha de forma explícita, sem tentativa e erro em produção.
- Competência sem arquivos ou com contagem inesperada: o resultado é uma falha de qualidade, não uma publicação vazia.
- Registro com chave de negócio duplicada: a política de deduplicação do dataset é aplicada e a ocorrência é contabilizada.
- Registro sem chave de negócio válida: é rejeitado e contabilizado, sem inferência silenciosa.
- Worker morre durante o commit ou depois de gravar dados antes de atualizar o metadado: a recuperação reconcilia dados e metadados e não publica estado parcial.
- Retry ocorre depois de a unidade já ter sido concluída: a operação é idempotente e não duplica registros.
- Duas execuções da mesma competência ocorrem ao mesmo tempo: a concorrência é detectada e coordenada; não há interleaving de versões.
- Publicação falha após a validação, mas antes da troca ativa: a versão anterior permanece ativa e a candidata fica identificada como não publicada.
- Consumidor lê durante a troca de versão: vê a versão anterior ou a nova completa, nunca uma mistura.
- Schema evolui com campo novo, removido ou com tipo alterado: a compatibilidade é avaliada; dados antigos continuam legíveis quando aplicável.
- Registro é removido da fonte: a remoção é tratada conforme o contrato do dataset, de forma auditável.
- Fonte publica novamente a mesma competência com conteúdo diferente: uma nova versão é criada; a anterior não é sobrescrita silenciosamente.
- Volume, memória ou disco excedem o limite: a execução falha de forma observável, preserva a versão anterior e permite retomada.
- Compactação, retenção ou expiração de versões falha: os dados ativos permanecem íntegros e a falha é registrada.
- CPF ou outro dado pessoal aparece em mensagem de erro, log ou rejeição: a saída é sanitizada e o incidente é registrado.

## Requirements *(mandatory)*

### Functional Requirements

#### Fonte, identidade e contrato

- **FR-001**: Cada execução MUST identificar a competência, a fonte, os arquivos de origem, seus tamanhos, hashes e contagens esperadas.
- **FR-002**: Arquivos de origem MUST ser imutáveis após a ingestão; uma republicação da mesma competência MUST gerar uma nova versão identificável, nunca sobrescrever silenciosamente a anterior.
- **FR-003**: Todo dataset MUST declarar sua identidade de registro e suas restrições de unicidade.
- **FR-004**: Deduplicação MUST usar chaves de negócio estáveis quando existirem; hashes de conteúdo MAY detectar mudanças, mas MUST NOT substituir a identidade.
- **FR-005**: Arquivo ou registro que não corresponda ao contrato MUST falhar ou ser rejeitado de forma explícita, com origem e motivo registrados.
- **FR-006**: Mudanças de schema MUST ser validadas antes da publicação; a versão anterior MUST permanecer legível quando a evolução for compatível.

#### Transações e publicação

- **FR-007**: Toda modificação persistente MUST ter limites transacionais explícitos.
- **FR-008**: Dados e metadados de conclusão de um lote MUST ser confirmados de forma coordenada; um lote MUST NOT ser marcado como concluído sem que seus dados estejam duramente confirmados.
- **FR-009**: Escritas parciais MUST NOT ser visíveis como publicação concluída.
- **FR-010**: A publicação de uma nova versão MUST ser atômica para os consumidores: eles MUST ver a versão anterior ou a nova versão completa, nunca uma mistura.
- **FR-011**: Quando o armazenamento não oferecer transações, um protocolo idempotente de publicação/compensação MUST ser documentado e testado.
- **FR-012**: A falha de qualquer unidade obrigatória MUST impedir a publicação da versão candidata.

#### Idempotência e concorrência

- **FR-013**: Ingestões, lotes e tarefas distribuídas MUST tolerar execução repetida sem duplicar registros ou efeitos colaterais.
- **FR-014**: Cada unidade lógica de trabalho MUST ter identidade determinística, reutilizada em retries.
- **FR-015**: Reexecutar uma unidade já concluída MUST ser seguro e MUST NOT alterar o resultado final.
- **FR-016**: Execuções concorrentes da mesma competência MUST ser impedidas ou coordenadas explicitamente; MUST NOT haver interleaving de versões.
- **FR-017**: Conflitos de concorrência MUST ser detectados, registrados e resolvidos por regra explícita.

#### Batches, checkpoints e execução distribuída

- **FR-018**: O trabalho MUST ser decomponível em partições independentes com propriedade determinística.
- **FR-019**: Cada partição/batch MUST ter manifesto com fonte, intervalo lógico, linhas esperadas/processadas, rejeições, status, erros e versão da lógica.
- **FR-020**: Partições MUST ter checkpoints duráveis que permitam retomada segura.
- **FR-021**: Trabalho concluído MUST NOT ser repetido desnecessariamente após falha ou reinício.
- **FR-022**: Falha de worker MUST liberar a propriedade da tarefa e permitir retry sem publicação parcial.
- **FR-023**: A concorrência MUST ser configurável e respeitar limites de workers, conexões e recursos.
- **FR-024**: O estado da orquestração e o commit dos dados MUST ser coordenados; uma tarefa só pode ser considerada concluída após seu efeito durável.
- **FR-025**: Tarefas MUST NOT pressupor memória compartilhada, ordem global ou estado em processo.

#### Incremental e deduplicação

- **FR-026**: A ingestão MUST evitar reescritas completas desnecessárias.
- **FR-027**: Quando a semântica da fonte permitir, o sistema MUST identificar registros novos, modificados, inalterados e removidos.
- **FR-028**: Mudanças MUST ser aplicadas de forma incremental sempre que for prático.
- **FR-029**: Uma carga completa MUST ser uma decisão explícita, justificada e registrada no histórico.
- **FR-030**: A lógica incremental MUST ser determinística e testada com dados novos, alterados, inalterados e removidos.
- **FR-031**: Remoções MUST ser tratadas conforme o contrato do dataset, de forma auditável.
- **FR-032**: Registros inalterados MUST NOT ser reescritos desnecessariamente quando a estratégia incremental permitir evitá-lo.

#### Recursos, qualidade e serving

- **FR-033**: Nenhum processamento MUST exigir carregar o conjunto completo em memória.
- **FR-034**: Streaming, chunking, particionamento, backpressure e apoio em disco MUST ser usados para grandes volumes.
- **FR-035**: Limites de memória, disco, conexões e concorrência MUST ser explícitos; ao atingir o limite, a execução MUST falhar de forma observável.
- **FR-036**: O tamanho e a quantidade de arquivos/partições MUST ser mantido sob controle por compactação e manutenção.
- **FR-037**: Antes da publicação, MUST haver validação de unicidade, contagens, referências e completude.
- **FR-038**: Registros rejeitados MUST ser contabilizados, classificados e auditáveis, sem exposição de CPF ou conteúdo sensível.
- **FR-039**: A camada de serving MUST ser atualizada somente a partir de uma versão canônica validada e publicada.
- **FR-040**: A troca da versão ativa do serving MUST ser atômica e registrada em metadados.

#### Observabilidade, auditoria e versionamento

- **FR-041**: Toda execução MUST expor status, fonte, contagens processadas/rejeitadas/retidas, retries, falhas e histórico.
- **FR-042**: Cada resultado MUST ser rastreável até a versão da fonte, versão da lógica/configuração, identificador da execução e versão publicada.
- **FR-043**: Logs MUST ser estruturados, correlacionáveis e livres de segredos, CPF e conteúdo integral de registros.
- **FR-044**: Métricas de duração, volume, rejeição, retry, recuperação e uso de recursos MUST ser registradas por etapa.
- **FR-045**: Versões publicadas MUST permitir auditoria e rollback; a retenção MUST seguir política explícita.
- **FR-046**: Metadados de versão MUST ser atualizados na mesma operação durável dos dados que descrevem.
- **FR-047**: A versão anterior MUST permanecer acessível durante a publicação e pelo período de retenção definido.
- **FR-048**: Compactação, expiração de versões e limpeza de temporários MUST ser observáveis e não podem comprometer a versão ativa.
- **FR-049**: Exceções arquiteturais MUST ser documentadas com justificativa, responsável e prazo de revisão.

### Key Entities

- **Manifesto da fonte**: competência, origem, lista de arquivos, tamanhos, hashes, contagens esperadas e versão da fonte.
- **Execução de ingestão**: identidade, status, início/fim, versão da lógica, configuração, totais e falhas.
- **Partição/batch**: unidade independente de trabalho, com intervalo lógico, propriedade, checkpoint, status, contagens e rejeições.
- **Dataset e identidade de registro**: conjunto de dados, chave de negócio, restrições de unicidade e regra de deduplicação.
- **Versão canônica**: snapshot imutável de um dataset, com origem, transformação, contagens e metadados de validação.
- **Publicação de serving**: versão canônica ativa, metadados de ativação e estado anterior para rollback.
- **Rejeição**: registro ou arquivo recusado, motivo classificável, origem e contagem, sem PII.
- **Checkpoint/lease**: estado durável de progresso e propriedade de uma partição.

### Fora de escopo

- Alterações na interface do portal, autenticação de usuários, API pública e dashboards de BI.
- Novas fontes de dados além do inventário atual da Receita Federal.
- Comparação histórica na interface ou séries temporais para o usuário final.
- Tempo real, CDC ou atualizações intradiárias.
- Escolha de produto específico de formato de tabela, catálogo ou engine; essa decisão pertence ao planejamento.
- Garantia de zero downtime multi-região; uma janela de manutenção documentada é aceitável no MVP.
- Provedor de armazenamento ou topologia física específica.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% das competências publicadas apresentam dados e metadados de conclusão consistentes; 0 leituras observam versão parcial ou misturada.
- **SC-002**: 100% das execuções repetidas da mesma unidade lógica produzem as mesmas contagens e 0 registros duplicados.
- **SC-003**: 100% das falhas antes da publicação preservam a versão anterior ativa; 0 lotes falhos são marcados como concluídos.
- **SC-004**: Em retomadas após falha, 100% das partições já confirmadas não são reprocessadas desnecessariamente.
- **SC-005**: O pipeline MUST suportar concorrência configurável de até 8 workers no ambiente alvo, com 0 corrupções e 0 duplicatas.
- **SC-006**: Com múltiplos workers no mesmo hardware e volume, o tempo total da competência MUST ser pelo menos 2× menor que o baseline de um worker, sem regressão de integridade.
- **SC-007**: 100% dos resultados publicados são rastreáveis a origem, hash da fonte, versão da lógica, execução e versão publicada.
- **SC-008**: 100% dos datasets publicados declaram identidade e unicidade; 0 chaves de negócio duplicadas aparecem na versão ativa.
- **SC-009**: 100% dos registros rejeitados são contabilizados e classificados; 0 rejeições silenciosas e 0 CPFs completos em logs, erros ou saídas de auditoria.
- **SC-010**: 100% das mudanças incompatíveis de schema/source falham antes da publicação; versões anteriores compatíveis permanecem legíveis.
- **SC-011**: 100% das publicações têm metadados de versão e rollback disponíveis; um rollback de teste restaura a versão anterior em ambiente controlado.
- **SC-012**: A carga da competência completa de referência (223M registros) MUST ser executada em até 2 horas no ambiente alvo com 8 workers, registrando duração, memória de pico e volume por etapa.

## Assumptions

- A fonte é o inventário mensal completo de CNPJ da Receita Federal. Não há CDC nem eventos incrementais por registro; o incremental é aplicado no nível de arquivo/partição e, quando praticável, por chave de negócio.
- O inventário de referência é a competência `2026-10`, com 37 arquivos e aproximadamente 223 milhões de registros.
- O pipeline pode usar uma janela de manutenção; consumidores podem ver a versão anterior até a troca atômica para a nova versão.
- O alvo inicial de concorrência é 8 workers, configurável; o número final e a meta de 2 horas serão validados por benchmark no ambiente alvo.
- O raw é preservado por competência; a retenção de versões e temporários segue política explícita e documentada.
- CPF de sócios é dado pessoal sob a LGPD e MUST permanecer mascarado em logs, exceções, rejeições e camadas de consulta.
- A escolha de formato de tabela, catálogo, engine de consulta e provedor de armazenamento será feita no planejamento, condicionada a PoC e aos requisitos desta spec.
- A camada de serving pode continuar dedicada ao portal/API de baixa latência; BI pode usar uma versão canônica separada.
- O volume pode crescer; os limites e metas devem ser revalidados quando a ordem de grandeza mudar.
- A feature não altera o escopo funcional do portal existente; ela muda como os dados chegam e são publicados.
