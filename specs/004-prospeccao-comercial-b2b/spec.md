# Feature Specification: Prospecção Comercial B2B

**Feature Branch**: `004-prospeccao-comercial-b2b`

**Created**: 2026-11-18

**Status**: Draft

**Input**: User description: "Gerar uma base de empresas qualificadas para prospecção
comercial, uma linha por estabelecimento (CNPJ completo) com o CNPJ raiz para agrupar
matriz e filiais, filtros por CNAE, localização, porte e situação, indicadores derivados
e geração de listas para abordagem. Fora do escopo: intenção de compra, orçamento ou
necessidade comercial."

## Contexto

O projeto já publica um explorador somente leitura dos dados públicos de CNPJ da
competência `2026-10`. Esta feature transforma o mesmo read model em uma **base de
prospecção**: a pessoa define um perfil comercial (segmento, região, porte, situação,
estrutura, contatos) e obtém a lista correspondente, pronta para enriquecimento e
abordagem.

A prospecção parte dos dados cadastrais da Receita Federal. O sistema **não** infere
intenção de compra, orçamento ou necessidade: esses critérios dependem de qualificação
posterior, fora deste escopo.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Gerar uma lista segmentada de empresas ativas (Priority: P1)

Como profissional de prospecção, quero filtrar empresas ativas por segmento, região,
porte e estrutura e gerar a lista correspondente, para iniciar uma campanha comercial
com empresas reais e compatíveis com um perfil definido.

**Why this priority**: É o critério de sucesso da feature. Sozinha, a geração da lista
já entrega valor: substitui planilhas manuais e permite começar a abordagem.

**Independent Test**: Abrir a página de prospecção, selecionar um segmento e uma UF,
confirmar a contagem, conferir os registros na tabela e baixar o CSV. Funciona sem
depender dos indicadores derivados ou das agregações.

**Acceptance Scenarios**:

1. **Given** a competência `2026-10` carregada, **When** a pessoa abre a página de
   prospecção, **Then** o formulário assume situação cadastral **Ativa** por padrão,
   exibe a competência e a fonte e informa que os contatos são do cadastro da Receita
   Federal.
2. **Given** um segmento e uma UF selecionados, **When** a consulta termina, **Then** a
   contagem e a listagem contêm apenas estabelecimentos ativos daquele segmento na UF,
   com CNPJ completo, razão social, nome fantasia, município, CNAE principal, porte,
   data de abertura, telefone e e-mail quando existirem.
3. **Given** um conjunto filtrado, **When** a pessoa aciona a geração da lista, **Then**
   o sistema entrega um CSV com uma linha por estabelecimento e o CNPJ raiz em coluna
   própria, respeitando os filtros ativos.
4. **Given** filtros que não retornam registros, **When** a consulta termina, **Then** a
   página mostra contagem zero e uma ação para limpar os filtros, sem tratar como erro.
5. **Given** um segmento inexistente ou um valor de filtro inválido, **When** a consulta
   é enviada, **Then** o sistema rejeita a combinação com mensagem explicativa e não
   executa a consulta.
6. **Given** mais registros do que o limite de exportação, **When** a lista é gerada,
   **Then** o CSV contém até o limite configurado e a página informa que o recorte foi
   limitado, sem truncar silenciosamente.

---

### User Story 2 - Avaliar o perfil e a estrutura de cada empresa (Priority: P2)

Como profissional de prospecção, quero ver indicadores derivados de cada estabelecimento
(idade, quantidade de CNAEs, quantidade de filiais, segmento, porte e natureza jurídica),
para priorizar contatos com base no perfil e no porte da operação.

**Why this priority**: A lista responde "quem"; os indicadores respondem "quem priorizar".
É a segunda camada de valor e depende apenas dos registros já carregados.

**Independent Test**: Consultar um CNPJ conhecido e conferir idade, quantidade de CNAEs,
quantidade de filiais e segmento calculados a partir dos dados da competência.

**Acceptance Scenarios**:

1. **Given** um estabelecimento com data de abertura, **When** o perfil é exibido,
   **Then** a idade é calculada em anos completos até o fim da competência, e "não
   informada" quando a data estiver ausente.
2. **Given** uma empresa com matriz e filiais, **When** o perfil é exibido, **Then** o
   estabelecimento mostra se é matriz ou filial, quantos estabelecimentos o CNPJ raiz
   possui e quantas filiais existem.
3. **Given** um estabelecimento com CNAE principal, **When** o perfil é exibido, **Then**
   o segmento comercial corresponde à divisão CNAE mapeada no catálogo de segmentos, e
   CNAEs fora do mapa aparecem como "Não classificado".
4. **Given** um estabelecimento sem telefone e sem e-mail, **When** o perfil é exibido,
   **Then** o contato aparece como não informado e não é inventado nem enriquecido.
5. **Given** um estabelecimento com CNAEs secundários, **When** o perfil é exibido,
   **Then** a quantidade de CNAEs inclui o principal e todos os secundários distintos.

---

### User Story 3 - Agrupar e comparar segmentos e regiões (Priority: P3)

Como gestor comercial, quero contar estabelecimentos por segmento e por UF dentro do recorte
atual, para decidir onde há volume suficiente antes de priorizar uma campanha.

**Why this priority**: Apoia a priorização, mas a feature já entrega valor com a lista e
o perfil. Pode ser adicionada depois sem bloquear as demais histórias.

**Independent Test**: Aplicar um recorte (por exemplo, situação ativa e porte) e conferir
que a soma dos grupos por segmento e por UF corresponde à contagem total do recorte.

**Acceptance Scenarios**:

1. **Given** um recorte ativo, **When** a página de prospecção é exibida, **Then** há
   uma tabela de contagem por segmento e outra por UF, ordenadas da maior para a menor.
2. **Given** a alteração de um filtro, **When** a página é atualizada, **Then** as duas
   agregações refletem exatamente o mesmo recorte da listagem.
3. **Given** um recorte sem resultados, **When** a página é exibida, **Then** as
   agregações ficam vazias, sem erro e sem linhas inventadas.

---

### Edge Cases

- **Dados indisponíveis**: se a competência não estiver publicada, a página informa a
  indisponibilidade e não mostra listas parciais.
- **Contato ausente ou desatualizado**: telefone e e-mail são apresentados como dados
  cadastrais da Receita Federal; o sistema não afirma que estão atualizados.
- **CPF de sócio**: nunca aparece em página, CSV, log ou mensagem de erro.
- **CNAE inválido/ausente**: o estabelecimento continua na lista com segmento
  "Não classificado" e sem CNAE principal.
- **Data de abertura ausente**: idade não informada; filtros de idade excluem o registro.
- **Empresa com muitas filiais**: contagem exibida por CNPJ raiz, sem duplicar linhas.
- **Recorte maior que o limite de exportação**: CSV limitado, com aviso explícito.
- **Caracteres especiais em nome/e-mail**: o CSV escapa aspas, vírgulas e quebras de
  linha; valores iniciados por `=`, `+`, `-` ou `@` são neutralizados para não virarem
  fórmula em planilhas.
- **Paginação**: os filtros permanecem ao navegar e ao voltar do detalhe.
- **Competência mensal**: a idade é calculada até o último dia da competência, não até
  a data corrente, para que o recorte seja reprodutível.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: O sistema MUST manter a granularidade de **uma linha por estabelecimento**
  (CNPJ completo) e MUST expor o CNPJ raiz (CNPJ básico) em todas as listas e no CSV.
- **FR-002**: O sistema MUST exibir, para cada estabelecimento, CNPJ completo, razão
  social, nome fantasia, UF, município, CEP, CNAE principal, situação cadastral, data de
  abertura, porte, natureza jurídica e capital social.
- **FR-003**: O sistema MUST exibir telefone e e-mail cadastrais quando existirem,
  identificando-os como provenientes da Receita Federal e potencialmente desatualizados.
- **FR-004**: O sistema MUST exibir a situação de optante do Simples Nacional e do MEI.
- **FR-005**: O sistema MUST calcular e exibir a idade da empresa em anos completos até o
  último dia da competência, com "não informada" quando não houver data de abertura.
- **FR-006**: O sistema MUST calcular e exibir a quantidade de CNAEs do estabelecimento,
  contando o principal e os secundários distintos.
- **FR-007**: O sistema MUST calcular e exibir a quantidade de estabelecimentos e de
  filiais do CNPJ raiz, sem duplicar a linha do estabelecimento.
- **FR-008**: O sistema MUST classificar cada estabelecimento em um segmento comercial a
  partir da divisão do CNAE principal, usando um catálogo explícito; CNAE ausente ou fora
  do catálogo MUST resultar em "Não classificado".
- **FR-009**: O sistema MUST permitir filtrar por segmento, UF, município, CNAE, porte,
  situação cadastral, matriz/filial, faixa de idade, faixa de filiais, Simples, MEI e
  presença de contato cadastral.
- **FR-010**: Os filtros MUST ser cumulativos; o sistema MUST aceitar no máximo um valor
  por dimensão.
- **FR-011**: O filtro de município MUST ser limitado à UF selecionada; ao trocar a UF,
  um município incompatível MUST ser removido com aviso.
- **FR-012**: O filtro de idade MUST aceitar mínima e máxima em anos, rejeitar mínima
  maior que a máxima e usar o fim da competência como referência.
- **FR-013**: O filtro de filiais MUST aceitar mínimo e máximo de filiais do CNPJ raiz,
  rejeitar mínimo maior que o máximo e MUST NOT contar a matriz como filial.
- **FR-014**: O filtro de contato MUST permitir "com contato", "sem contato" ou
  "qualquer", considerando telefone ou e-mail cadastral presente.
- **FR-015**: A página de prospecção MUST iniciar com situação cadastral **Ativa**, sem
  impedir que a pessoa escolha "Todas" ou outra situação.
- **FR-016**: O sistema MUST exibir a contagem total do recorte e manter o limite máximo
  de contagem já vigente no portal (50.000), sinalizando "Mais de N" quando aplicável.
- **FR-017**: O sistema MUST exibir os resultados em tabela paginada, com ordenação
  estável por CNPJ completo, preservando os filtros entre páginas.
- **FR-018**: O sistema MUST exibir a contagem de estabelecimentos (uma linha por
  estabelecimento, sem deduplicar por CNPJ raiz) por segmento e por UF para o
  mesmo recorte dos filtros ativos.
- **FR-019**: O sistema MUST permitir baixar um CSV do recorte filtrado, com uma linha por
  estabelecimento, cabeçalho em português e separador compatível com planilhas.
- **FR-020**: O CSV MUST conter CNPJ completo, CNPJ raiz, razão social, nome fantasia,
  segmento, situação, UF, município, CEP, CNAE principal, CNAEs secundários, quantidade
  de CNAEs, porte, natureza jurídica, capital social, data de abertura, idade,
  matriz/filial, quantidade de estabelecimentos, quantidade de filiais, telefone,
  e-mail, Simples e MEI.
- **FR-021**: O CSV MUST respeitar os filtros ativos e MUST ser limitado por um teto
  configurável (padrão 50.000 linhas), informando na página quando o recorte exceder o
  teto.
- **FR-022**: O CSV MUST neutralizar valores que comecem com `=`, `+`, `-` ou `@` e MUST
  escapar aspas, vírgulas, ponto e vírgula e quebras de linha.
- **FR-023**: O sistema MUST NOT exportar CPF, dados de sócios ou qualquer dado que não
  esteja no read model de estabelecimentos.
- **FR-024**: O sistema MUST rejeitar filtros inválidos (segmento desconhecido, intervalo
  invertido, número fora da faixa) com mensagem explicativa, sem executar a consulta.
- **FR-025**: O sistema MUST NOT inferir intenção de compra, orçamento, necessidade ou
  propensão comercial, nem apresentar tais indicadores como disponíveis.
- **FR-026**: O sistema MUST NOT alterar ou excluir registros; a prospecção é somente
  leitura.
- **FR-027**: O sistema MUST calcular os indicadores a partir da competência carregada e
  MUST NOT combiná-los com dados de outra competência ou de fontes externas.
- **FR-028**: O sistema MUST manter a semântica de optante do Simples/MEI já usada no
  portal (opção ativa no fim da competência).
- **FR-029**: O CSV MUST ser gerado em streaming, sem carregar todo o recorte em memória.
- **FR-030**: O sistema MUST registrar apenas rota, status e duração das requisições,
  nunca valores de filtros, contatos ou CPF.

### Key Entities

- **Estabelecimento**: linha da base de prospecção, identificada pelo CNPJ completo.
  Contém dados cadastrais, contatos, situação, CNAEs, segmento e indicadores derivados.
- **Empresa (CNPJ raiz)**: agrupador de matriz e filiais. Contém razão social, natureza
  jurídica, porte, capital social e a contagem de estabelecimentos/filiais.
- **Segmento comercial**: classificação derivada da divisão CNAE (dois primeiros dígitos)
  em um catálogo explícito de segmentos de mercado.
- **Indicadores derivados**: idade da empresa, quantidade de CNAEs, quantidade de
  estabelecimentos, quantidade de filiais e presença de contato.
- **Recorte de prospecção**: combinação de filtros que define a lista a ser gerada e
  exportada.
- **Lista de prospecção (CSV)**: arquivo com uma linha por estabelecimento do recorte,
  limitado por teto configurável.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Uma pessoa consegue sair da página inicial de prospecção para um CSV com
  pelo menos 100 empresas ativas de um segmento e UF em menos de 2 minutos, sem planilha
  intermediária.
- **SC-002**: 100% das linhas do CSV contêm CNPJ completo de 14 dígitos e CNPJ raiz de 8
  dígitos, permitindo agrupar matriz e filiais.
- **SC-003**: A contagem exibida e a soma dos grupos por segmento e por UF correspondem
  ao mesmo recorte em 100% dos casos testados.
- **SC-004**: Nenhuma resposta do portal ou do CSV contém CPF completo.
- **SC-005**: A listagem filtrada responde em até 5 s (p95) no ambiente de referência e o
  CSV de 1.000 linhas é gerado em até 10 s.
- **SC-006**: 100% dos estabelecimentos com data de abertura exibem idade calculada até o
  fim da competência, e os demais exibem "não informada".

## Assumptions

- A competência publicada é `2026-10`; a feature não introduz histórico nem seletor de
  competência.
- O catálogo de segmentos é uma decisão de produto versionada em código, não um dado da
  Receita Federal; sua revisão gera nova carga do read model.
- Telefone e e-mail são os do cadastro da Receita Federal; não há enriquecimento nem
  verificação de contato nesta feature.
- O teto de exportação padrão (50.000 linhas) é suficiente para o MVP e configurável.
- A base real completa (≈73,8 M estabelecimentos) usa o mesmo read model; a feature não
  cria uma segunda cópia dos dados.
