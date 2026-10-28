# Feature Specification: Explorador Visual de Dados CNPJ

**Feature Branch**: `001-cnpj-query-screen`

**Created**: 2026-10-22

**Updated**: 2026-10-27

**Status**: Draft

**Input**: User description: "Atualização do MVP para um explorador visual dos dados públicos de CNPJ, com visualização e filtragem da competência disponível, detalhes de estabelecimento e sem exportação ou download."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Explorar e filtrar os registros da competência disponível (Priority: P1)

Como pessoa interessada nos dados públicos de CNPJ, quero consultar o retrato disponível, combinar filtros e navegar por uma listagem paginada, para encontrar os estabelecimentos que procuro sem precisar abrir planilhas ou baixar arquivos.

**Why this priority**: É o núcleo do MVP. Sozinha, a exploração já entrega valor: mostra quais dados existem, permite reduzir o conjunto por critérios reais e informa quantos registros correspondem. Também estabelece a base para abrir os detalhes.

**Independent Test**: Abrir o site, confirmar a competência e a fonte exibidas, aplicar uma combinação de filtros (por exemplo, UF, situação cadastral e CNAE), ver a contagem total e navegar entre as páginas. Funciona sem depender da tela de detalhes.

**Acceptance Scenarios**:

1. **Given** o inventário da competência `2026-10` está disponível, **When** a pessoa abre o explorador, **Then** a tela identifica claramente a competência `2026-10` e a fonte `Receita Federal`, e deixa explícito que se trata de um retrato mensal, não de dados em tempo real.
2. **Given** a listagem inicial, **When** a pessoa aplica filtros de situação cadastral, UF, município, CNAE principal ou secundário, porte, matriz/filial, data de abertura e opção pelo Simples ou MEI, **Then** a contagem total e os registros exibidos correspondem a todos os filtros ativos.
3. **Given** um resultado com mais registros do que uma página, **When** a pessoa navega para a próxima página, **Then** os filtros permanecem aplicados e a página exibe o conjunto correspondente, com indicação da página atual.
4. **Given** uma combinação de filtros aplicada, **When** a pessoa altera ou remove um filtro, **Then** a contagem e a listagem são atualizadas e a navegação retorna à primeira página.
5. **Given** uma combinação de filtros sem correspondências, **When** a consulta termina, **Then** a tela informa que não há resultados, mostra contagem zero e oferece uma ação clara para limpar os filtros.
6. **Given** um estabelecimento com situação cadastral não ativa, **When** ele atende aos filtros, **Then** ele continua visível na listagem com sua situação sinalizada.
7. **Given** uma busca por CNPJ ou nome, **When** a pessoa informa o termo, **Then** a busca atua como apoio à filtragem: o CNPJ completo é localizado com ou sem máscara e o nome localiza registros que contenham o termo, sem corrigir erros de digitação nem retornar correspondências aproximadas.

---

### User Story 2 - Consultar os detalhes de um estabelecimento (Priority: P2)

Como pessoa interessada nos dados públicos de CNPJ, quero abrir um estabelecimento da listagem e consultar seus dados cadastrais, os dados da empresa e a situação de Simples/MEI, para confirmar se encontrei o registro certo e entender seu contexto.

**Why this priority**: A listagem responde "quais registros existem"; os detalhes respondem "o que este registro é". É a segunda camada de valor e depende apenas de um registro previamente localizado.

**Independent Test**: A partir de um resultado da listagem, abrir o detalhe de um estabelecimento e verificar dados do estabelecimento, da empresa e do Simples/MEI. Pode ser testado com um registro conhecido, independentemente de outros filtros.

**Acceptance Scenarios**:

1. **Given** um estabelecimento exibido na listagem, **When** a pessoa abre seus detalhes, **Then** vê os dados do estabelecimento, os dados da empresa e a situação de Simples/MEI.
2. **Given** um estabelecimento com endereço, CNAEs ou contatos cadastrais, **When** os detalhes são exibidos, **Then** esses dados aparecem quando disponíveis e os contatos são identificados como provenientes do cadastro da Receita Federal, podendo estar ausentes ou desatualizados.
3. **Given** uma empresa com sócios registrados, **When** os detalhes são exibidos, **Then** os sócios aparecem com nome ou razão social, qualificação, data de entrada e país, e o CPF é sempre exibido de forma mascarada.
4. **Given** uma empresa sem sócios, sem CNAE secundário ou sem registro de Simples/MEI, **When** os detalhes são exibidos, **Then** as seções correspondentes indicam ausência de informação ou são omitidas, sem erro e sem campos quebrados.
5. **Given** a pessoa está vendo os detalhes de um estabelecimento, **When** retorna à listagem, **Then** os filtros e a página anteriormente selecionados são preservados.

---

### Edge Cases

- A competência disponível não está acessível ou o inventário está vazio: a tela informa a indisponibilidade e não exibe resultados parciais, de outra competência ou uma listagem vazia enganosa.
- CNPJ informado na busca é inválido (letras, quantidade incorreta de dígitos ou dígitos verificadores inválidos): a busca é rejeitada com mensagem explicativa, sem alterar os resultados exibidos.
- CNPJ válido não existe no inventário: a tela informa que não há correspondência e oferece limpar ou ajustar a busca.
- Data inicial do filtro de abertura é posterior à data final: a combinação é rejeitada com mensagem explicativa, sem executar a consulta.
- Município selecionado deixa de ser compatível com a UF após uma troca de UF: o município é removido e a tela informa o ajuste, sem manter um filtro impossível.
- A página solicitada deixa de existir após uma alteração de filtros: a listagem retorna a uma página válida e mantém os filtros ativos.
- A busca por nome não encontra correspondência por causa de erro de digitação: o resultado é vazio; não há correção automática nem sugestão aproximada.
- Várias empresas ou estabelecimentos têm o mesmo nome: a listagem os distingue pelo CNPJ completo, UF, município e situação cadastral.
- Estabelecimento com situação cadastral baixada, suspensa, inapta ou nula: permanece visível quando atende aos filtros, com a situação sinalizada.
- Empresa sem sócios, sem CNAE secundário, sem contato ou sem registro de Simples/MEI: a interface trata a ausência sem erro e sem seções quebradas.
- Contato cadastral ausente ou desatualizado: a interface não o apresenta como dado confirmado nem como contato enriquecido.
- Sócio com CPF: o CPF nunca aparece completo em listagem, detalhe, mensagem de erro ou registro de aplicação.
- A pessoa altera os filtros enquanto uma consulta anterior ainda está em andamento: a tela exibe apenas os resultados da combinação mais recente, sem misturar respostas atrasadas.
- Uma combinação de filtros válida não retorna nenhum registro: o estado é de resultado vazio, não de erro.
- O volume de resultados é muito grande: a paginação e a contagem continuam utilizáveis, e a tela apresenta estado de carregamento enquanto a consulta não termina.
- A consulta falha por indisponibilidade ou tempo esgotado: a tela mostra mensagem compreensível, sem detalhe interno, e permite tentar novamente.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: O sistema MUST exibir, de forma persistente na listagem e nos detalhes, a competência dos dados (`2026-10`) e a fonte (`Receita Federal`).
- **FR-002**: O sistema MUST deixar explícito que os dados são um retrato mensal, não um dado em tempo real, e MUST NOT apresentar comparação histórica entre competências.
- **FR-003**: O sistema MUST operar sobre a competência disponível no inventário atual (`2026-10`), sem seletor de competência e sem prometer a existência de outras competências.
- **FR-004**: O sistema MUST permitir combinar, em uma mesma exploração, filtros de situação cadastral, UF, município, CNAE principal ou secundário, porte, matriz/filial, data de abertura, opção pelo Simples Nacional e opção pelo MEI.
- **FR-005**: Os filtros ativos MUST ser cumulativos: um estabelecimento só é exibido quando atende a todos os critérios selecionados. O MVP não exige seleção de múltiplos valores no mesmo filtro.
- **FR-006**: O filtro de CNAE MUST corresponder ao CNAE principal ou a qualquer CNAE secundário do estabelecimento.
- **FR-007**: O filtro de município MUST ser limitado à UF selecionada; ao trocar a UF, o sistema MUST remover um município incompatível e informar o ajuste.
- **FR-008**: O filtro de data de abertura do estabelecimento MUST aceitar um intervalo com data inicial e final inclusivas; uma data inicial posterior à final MUST ser rejeitada com mensagem explicativa e sem consulta.
- **FR-009**: O sistema MUST oferecer busca por CNPJ ou nome como apoio à filtragem: a busca por CNPJ MUST localizar o CNPJ completo informado, com ou sem máscara, e a busca por nome MUST localizar registros cujo nome empresarial contenha o termo informado.
- **FR-010**: A busca por nome MUST ser textual, sem diferenciar maiúsculas/minúsculas ou acentuação, mas MUST NOT corrigir erros de digitação nem usar correspondência aproximada.
- **FR-011**: O sistema MUST validar o CNPJ informado na busca e, quando o formato ou os dígitos verificadores forem inválidos, exibir mensagem explicativa sem executar a consulta.
- **FR-012**: O sistema MUST exibir a quantidade total de estabelecimentos correspondentes aos filtros ativos, não apenas a quantidade da página atual.
- **FR-013**: O sistema MUST exibir os registros correspondentes em uma listagem paginada, com navegação entre páginas e indicação da página atual.
- **FR-014**: O sistema MUST preservar os filtros ativos ao navegar entre páginas.
- **FR-015**: Ao alterar qualquer filtro, o sistema MUST atualizar a contagem e a listagem e retornar à primeira página.
- **FR-016**: Após uma alteração de filtros, a tela MUST exibir apenas os resultados da combinação mais recente, mesmo que uma consulta anterior demore mais para responder.
- **FR-017**: A listagem MUST exibir estabelecimentos e, para cada um, CNPJ completo, nome empresarial, nome fantasia quando houver, situação cadastral, UF, município, CNAE principal, indicador de matriz/filial, data de abertura e indicador de opção pelo Simples/MEI.
- **FR-018**: A listagem MUST incluir estabelecimentos com situação cadastral não ativa quando eles atenderem aos filtros, sinalizando a situação.
- **FR-019**: O sistema MUST permitir abrir os detalhes cadastrais de um estabelecimento a partir da listagem.
- **FR-020**: Os detalhes MUST exibir os dados do estabelecimento: CNPJ completo, matriz/filial, situação e data da situação cadastral, data de abertura, endereço, CNAE principal, CNAEs secundários e contatos cadastrais quando disponíveis.
- **FR-021**: Os contatos exibidos MUST ser identificados como provenientes do cadastro da Receita Federal e MUST NOT ser apresentados como dados enriquecidos, confirmados ou atualizados em tempo real.
- **FR-022**: Os detalhes MUST exibir os dados da empresa: razão social, natureza jurídica, porte, capital social, qualificação do responsável, CNAE principal e demais dados cadastrais disponíveis.
- **FR-023**: Os detalhes MUST exibir a situação de opção pelo Simples Nacional e pelo MEI, com datas de opção e exclusão quando disponíveis, ou indicar a ausência de registro.
- **FR-024**: Os detalhes MUST exibir os sócios quando disponíveis, com nome ou razão social, qualificação, data de entrada e país.
- **FR-025**: O sistema MUST mascarar o CPF dos sócios em qualquer listagem, detalhe, mensagem de erro ou registro de aplicação, exibindo apenas uma forma parcial ou totalmente mascarada e nunca os 11 dígitos.
- **FR-026**: Ao retornar dos detalhes para a listagem, o sistema MUST preservar os filtros e a página anteriormente selecionados.
- **FR-027**: Quando não houver resultados, o sistema MUST exibir contagem zero, mensagem clara e ação para limpar os filtros, sem tratar o caso como erro.
- **FR-028**: Quando os dados da competência não estiverem disponíveis, o sistema MUST informar a indisponibilidade e MUST NOT exibir resultados parciais ou de outra competência.
- **FR-029**: O sistema MUST apresentar estado de carregamento durante a consulta e, em caso de falha, mensagem compreensível, sem detalhe interno, com opção de tentar novamente.
- **FR-030**: Campos opcionais ausentes MUST ser omitidos ou indicados como não informados, sem quebrar a listagem ou os detalhes.
- **FR-031**: O sistema MUST usar os valores e descrições presentes nos dados disponíveis, nas tabelas de apoio e no catálogo de domínio para compor filtros e exibições, sem inventar categorias ou apresentar valores de outra competência.
- **FR-032**: O sistema MUST ser somente leitura: MUST NOT permitir editar, excluir, enviar ou corrigir registros.
- **FR-033**: O sistema MUST NOT oferecer exportação, download de arquivos, geração de CSV ou qualquer ação equivalente no MVP.
- **FR-034**: O sistema MUST NOT apresentar como disponíveis dados de Lucro Real, Lucro Presumido, Lucro Arbitrado, PGFN, CNO, contatos enriquecidos ou qualquer fonte externa ao inventário atual.
- **FR-035**: Nos filtros de Simples Nacional e MEI, "optante" MUST significar opção registrada sem exclusão até o fim da competência, "não optante" MUST significar ausência de opção ou exclusão registrada até o fim da competência, e "qualquer" MUST não restringir o resultado.
- **FR-036**: A listagem MUST usar uma ordenação estável por CNPJ completo, para que a navegação entre páginas não repita nem omita registros; o MVP não exige que a pessoa escolha a ordenação.

### Key Entities

- **Competência**: mês e ano de referência do retrato de dados. O inventário atual contém `2026-10`; a competência e a fonte (`Receita Federal`) acompanham todas as telas que exibem dados.
- **Empresa**: pessoa jurídica identificada pelo CNPJ básico. Inclui razão social, natureza jurídica, porte, capital social, qualificação do responsável, CNAE principal, ente federativo e relacionamentos com estabelecimentos, sócios e Simples/MEI.
- **Estabelecimento**: endereço operacional da empresa, identificado pelo CNPJ completo. Inclui indicador de matriz ou filial, nome fantasia, situação cadastral e data da situação, data de abertura, endereço, CNAE principal, CNAEs secundários e contatos cadastrais.
- **Sócio**: vínculo entre uma empresa e uma pessoa ou entidade. Inclui nome ou razão social, CPF mascarado, qualificação, data de entrada, país, faixa etária e representante legal, quando aplicável.
- **Simples/MEI**: situação de opção pelo Simples Nacional e pelo MEI, com datas de opção e exclusão quando existirem. Pode não haver registro para uma empresa.
- **Tabela de apoio**: descrições de domínio usadas nos filtros e nas telas, como CNAE, município, natureza jurídica, qualificação, motivo de situação e país.
- **Critérios de exploração**: conjunto de filtros ativos e termo de busca por CNPJ ou nome que delimita os registros exibidos.
- **Resultado da exploração**: quantidade total de correspondências, página atual e registros exibidos, acompanhados da competência e da fonte.

### Fora de escopo do MVP

- Exportação de resultados, download de arquivos e geração de CSV. Exportação CSV fica registrada como melhoria futura.
- API pública para terceiros, integração com aplicativos e contratos de API versionados.
- Comparação histórica entre competências, seleção de snapshots ou séries temporais.
- Dashboards, painéis de agregados, relatórios, visões personalizadas e ferramentas de BI.
- Busca tolerante a erros de digitação, correspondência aproximada e ordenação por similaridade.
- Inteligência artificial, recomendações e assistentes.
- Cobrança, planos, assinaturas e qualquer modelo comercial.
- Autenticação, contas, perfis e permissões de usuário.
- Integração com fontes externas e enriquecimento de contatos.
- Dados de Lucro Real, Lucro Presumido, Lucro Arbitrado, PGFN e CNO, que não fazem parte do inventário atual.
- Edição, exclusão, upload ou correção de dados pela interface.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% das telas que exibem dados identificam a competência `2026-10` e a fonte `Receita Federal`; nenhuma tela sugere dado em tempo real ou comparação histórica.
- **SC-002**: 95% das consultas com filtros exibem a contagem total e a primeira página em até 5 segundos, considerando o volume da competência `2026-10`.
- **SC-003**: 95% das aberturas de detalhes de estabelecimento exibem os dados em até 3 segundos.
- **SC-004**: 90% dos participantes de um teste de usabilidade, sem perfil previamente definido, concluem na primeira tentativa a tarefa "filtrar por UF e CNAE, abrir um estabelecimento e identificar sua situação e opção pelo Simples/MEI", sem ajuda.
- **SC-005**: 100% dos CPFs exibidos em telas e mensagens de erro estão mascarados; nenhum CPF completo aparece em registro de aplicação.
- **SC-006**: 0 telas ou ações oferecem exportação, download de arquivos ou geração de CSV no MVP.
- **SC-007**: 100% das listagens paginadas preservam os filtros ativos ao navegar e 100% das alterações de filtro retornam à primeira página.
- **SC-008**: 100% dos estados sem resultados exibem contagem zero e uma ação para limpar os filtros; 100% dos CNPJs e intervalos de data inválidos são rejeitados sem alterar os resultados.
- **SC-009**: 100% das linhas da listagem exibem CNPJ, UF, município e situação cadastral, permitindo distinguir registros com nomes iguais.
- **SC-010**: 0 telas apresentam como disponíveis dados de Lucro Real, Lucro Presumido, Lucro Arbitrado, PGFN, CNO, contatos enriquecidos ou comparação histórica.
- **SC-011**: Ao percorrer todas as páginas com a mesma combinação de filtros, 0 registros são repetidos ou omitidos.

## Assumptions

- O público-alvo ainda não está definido; a especificação não otimiza a experiência para um perfil profissional específico e usa "pessoa interessada" de forma neutra.
- O inventário atual contém apenas a competência `2026-10`; não há outros snapshots disponíveis e, portanto, não há comparação histórica no MVP.
- A fonte dos dados é o cadastro público de CNPJ da Receita Federal, já disponível no inventário produzido pelo projeto. A feature não coleta, complementa ou altera os dados de origem; pode preparar uma camada de consulta derivada exclusivamente do inventário atual.
- A competência é um retrato mensal e pode não refletir alterações cadastrais ocorridas depois da sua geração.
- Os dados disponíveis abrangem exclusivamente empresas, estabelecimentos, sócios, Simples/MEI e tabelas de apoio do inventário atual; nenhuma outra base é consultada. Sócios são exibidos quando existirem, com CPF mascarado.
- Contatos cadastrais vêm do cadastro da Receita Federal, podem estar ausentes ou desatualizados e não são enriquecidos por outras fontes.
- O MVP é somente para visualização e filtragem; não há exportação, download, edição ou upload.
- Não há login, conta ou perfil de usuário no MVP. O acesso ocorre por navegador, sem instalação de aplicativo.
- A busca por CNPJ ou nome é um apoio à filtragem. Ela não é tolerante a erros de digitação e não usa similaridade.
- O volume de dados é da ordem de dezenas de milhões de estabelecimentos; a paginação e os critérios de desempenho consideram esse volume.
- A aplicação não altera o pipeline de ingestão nem cria novas fontes de dados; consome somente o inventário atual, podendo manter uma camada de consulta derivada para desempenho.
- A camada de consulta deve permanecer desacoplada da interface web, para permitir uma futura API para terceiros e ferramentas de BI sem refazer a carga nem duplicar regras de negócio.
- Descrições de códigos oficiais de domínio (situação cadastral, porte, matriz/filial e Simples/MEI) podem vir de um catálogo versionado do próprio projeto, sem ampliar as fontes de dados.
- As opções de filtro refletem os valores existentes no inventário e nas tabelas de apoio da competência `2026-10`.
