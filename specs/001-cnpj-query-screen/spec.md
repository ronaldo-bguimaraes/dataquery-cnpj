# Feature Specification: Tela de Consulta de Dados CNPJ

**Feature Branch**: `001-cnpj-query-screen`

**Created**: 2026-10-22

**Status**: Draft

**Input**: User description: "bora criar a tela disso"

**Escopo da v1**: consulta por CNPJ com ficha completa da empresa e busca por filtros.
Exportação de resultados e painel de agregados estão **fora** desta versão e serão
tratados em specs separados.

## User Scenarios & Testing *(mandatory)*

<!--
  IMPORTANT: User stories should be PRIORITIZED as user journeys ordered by importance.
  Each user story/journey must be INDEPENDENTLY TESTABLE - meaning if you implement just ONE of them,
  you should still have a viable MVP (Minimum Viable Product) that delivers value.
-->

### User Story 1 - Consultar ficha de uma empresa pelo CNPJ (Priority: P1)

Como analista de dados, quero digitar o CNPJ de uma empresa e ver a ficha completa dela
(cadastro, endereços, sócios e regime tributário) para responder rapidamente a uma
pergunta sobre uma empresa específica, sem abrir planilha ou terminal.

**Why this priority**: É a consulta de maior valor e menor risco: um CNPJ é a chave natural
do domínio e existe um único registro por empresa. Entrega utilidade real sozinha e
valida a leitura do dataset ingerido.

**Independent Test**: Abrir a tela, digitar um CNPJ válido existente e ver a ficha completa
com cadastro, endereços, sócios e regime — funciona sem nenhum outro recurso implementado.

**Acceptance Scenarios**:

1. **Given** uma empresa existente na base, **When** o usuário digita o CNPJ com ou sem
   máscara e confirma a busca, **Then** a ficha da empresa é exibida com razão social,
   natureza jurídica, capital social, porte, CNAE principal e data de início de atividade.
2. **Given** uma empresa existente na base, **When** o usuário digita o CNPJ, **Then** são
   listados seus estabelecimentos (matriz e filiais) com endereço completo, CNAE e situação
   cadastral de cada um.
3. **Given** uma empresa existente na base, **When** o usuário digita o CNPJ, **Then** são
   listados seus sócios com nome, qualificação e data de entrada na sociedade.
4. **Given** uma empresa existente na base, **When** o usuário digita o CNPJ, **Then** é
   exibida a situação do Simples Nacional e do MEI, com as datas de opção e exclusão quando
   existirem.
5. **Given** qualquer resultado exibido, **When** o usuário observa a tela, **Then** a
   competência (mês e ano) dos dados exibidos está visível.

---

### User Story 2 - Buscar empresas por filtros (Priority: P2)

Como analista de dados, quero filtrar empresas por razão social, município, UF, CNAE e
porte para montar uma lista de empresas que atenda a um critério de negócio, por exemplo
todas as empresas de um CNAE em uma cidade.

**Why this priority**: É o caminho para o uso em volume e o que diferencia a tela de uma
simples busca por chave. Depende só da leitura do dataset, sem depender do P1 estar pronto
para entregar valor de forma independente.

**Independent Test**: Aplicar um filtro de CNAE e município, ver a lista paginada de
empresas correspondentes e navegar entre as páginas — entrega valor sem o P1 implementado.

**Acceptance Scenarios**:

1. **Given** um conjunto de empresas na base, **When** o usuário informa CNAE, município e
   porte e confirma, **Then** a lista exibe somente as empresas que atendem a todos os
   filtros aplicados.
2. **Given** uma busca com filtros, **When** o usuário altera um filtro, **Then** a lista é
   recalculada e a contagem total de resultados é atualizada.
3. **Given** uma busca que retorna muitos resultados, **When** o usuário navega entre as
   páginas, **Then** ele percorre todos os resultados sem perder os filtros aplicados.
4. **Given** uma busca sem resultados, **When** a consulta é concluída, **Then** a tela
   informa que não houve correspondência e sugere remover os filtros.
5. **Given** uma busca por razão social com erro de digitação, **When** o usuário confirma,
   **Then** a empresa pretendida aparece entre os resultados, ordenada por similaridade da
   correspondência.

---

### Edge Cases

- CNPJ digitado inválido (dígitos verificadores incorretos, letras, número incompleto):
  a tela rejeita a entrada com mensagem explicativa e não dispara consulta.
- CNPJ válido porém inexistente na base: a tela informa que a empresa não foi encontrada
  e oferece deixar a busca aberta para nova tentativa.
- CNPJ digitado com máscara, com barras, com espaços ou só com dígitos: todas as formas
  precisam ser aceitas e normalizadas antes da consulta.
- Razão social com acentuação divergente, abreviações ou razão social idêntica em empresas
  diferentes: a listagem precisa distinguir as empresas pelo CNPJ exibido.
- Busca por razão social com erro de digitação ou abreviação ("PETRBRAS", "LTDA"): a
  empresa pretendida precisa aparecer mesmo assim, e a ordenação por relevância precisa
  colocar as correspondências mais próximas primeiro.
- Busca por radical genérico que casa com muitas empresas ("PETRO"): a listagem precisa
  apresentar, junto do nome, o CNPJ, a UF e o município para que o usuário distinga as
  empresas, e a ordenação por relevância precisa ser preservada entre as páginas.
- Buscas com muitas combinações de filtro que não retornam nada: tratar como resultado
  vazio, sem erro.
- Estabelecimento com situação cadastral encerrada: continua visível na listagem, com a
  situação sinalizada.
- Empresa sem sócios registrados, sem CNAE secundário ou sem registro de Simples/MEI:
  a ficha mostra as seções como vazias, sem erro e sem campos quebrados.
- CPF de sócio: nunca deve ser exibido por completo em nenhuma tela, listagem ou mensagem
  de erro, mesmo sem autenticação no acesso.
- Dado de competência antiga: a tela deve deixar claro de qual mês/ano é o dado exibido,
  para o usuário não tomar decisão com dado defasado.
- Concorrência entre a mesma consulta disparada por dois usuários: as respostas não podem
  se misturar entre requisições diferentes.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: O sistema MUST oferecer um campo de busca por CNPJ que aceite o número com ou
  sem máscara e normalizar a entrada antes da consulta.
- **FR-002**: O sistema MUST validar o formato do CNPJ digitado e, quando inválido, exibir
  mensagem explicativa sem executar a consulta.
- **FR-003**: O sistema MUST exibir, para a empresa consultada, os dados de cadastro: razão
  social, natureza jurídica, capital social, porte, CNAE principal e data de início de
  atividade.
- **FR-004**: O sistema MUST exibir os estabelecimentos da empresa, identificando matriz e
  filiais, com endereço, CNAE e situação cadastral.
- **FR-005**: O sistema MUST exibir os sócios da empresa com nome, qualificação e data de
  entrada na sociedade.
- **FR-006**: O sistema MUST exibir a situação de Simples Nacional e MEI da empresa, com as
  datas de opção e exclusão quando disponíveis.
- **FR-007**: O sistema MUST exibir de forma visível a competência (mês e ano) dos dados
  apresentados em qualquer tela.
- **FR-008**: O sistema MUST permitir buscar empresas por combinação de filtros: razão
  social, UF, município, CNAE e porte.
- **FR-009**: O sistema MUST permitir a busca por CNAE de forma hierárquica, localizando
  empresas cujo CNAE principal ou secundário pertença ao grupo informado.
- **FR-010**: O sistema MUST exibir o total de resultados de uma busca com filtros e
  paginar a listagem, preservando os filtros entre as páginas.
- **FR-011**: O sistema MUST permitir a ordenação da listagem de resultados por critério
  escolhido pelo usuário.
- **FR-012**: O sistema MUST executar a busca por razão social de forma tolerante a erros de
  digitação e a diferenças de acentuação e caixa, ordenando os resultados por relevância da
  correspondência.
- **FR-013**: O sistema MUST sinalizar na listagem o trecho da razão social que originou a
  correspondência, para que o usuário compreenda o porquê do resultado.
- **FR-014**: O sistema MUST apresentar na listagem, junto da razão social, o CNPJ, a UF e o
  município de cada empresa, para permitir distinguir empresas de nomes semelhantes.
- **FR-015**: O sistema MUST apresentar estado vazio com orientação acionável quando uma
  busca não retorna resultados.
- **FR-016**: O sistema MUST apresentar mensagem de erro compreensível, sem expor detalhe
  interno da plataforma, quando uma consulta falhar.
- **FR-017**: O sistema MUST mascarar o CPF dos sócios em qualquer exibição ou mensagem de
  erro.
- **FR-018**: O sistema MUST estar acessível a qualquer usuário da rede interna sem exigir
  credencial, e MUST NOT registrar em log o CPF, o nome do sócio ou o conteúdo completo de
  uma ficha exibida.
- **FR-019**: O sistema MUST NOT oferecer, nesta versão, exportação de resultados nem painel
  de agregados.

### Key Entities

- **Empresa**: razão social, CNPJ, natureza jurídica, capital social, porte, CNAE principal,
  data de início de atividade. Uma empresa possui vários estabelecimentos e vários sócios.
- **Estabelecimento**: endereço (logradouro, número, bairro, CEP, UF, município), CNAE
  principal e secundário, situação cadastral, data de início de atividade, indicador de
  matriz ou filial. Pertence a uma empresa.
- **Sócio**: nome, CPF, qualificação, data de entrada na sociedade, país, faixa etária,
  representante legal. Pertence a uma empresa.
- **Regime Tributário**: opção e data de opção do Simples Nacional, opção e data de opção do
  MEI, datas de exclusão. Associado a uma empresa.
- **Filtro de Busca**: conjunto de critérios combináveis (razão social, UF, município, CNAE,
  porte) que delimita um conjunto de empresas, com ordenação e paginação.
- **Resultado de Busca**: lista paginada de empresas que atendem a um filtro, acompanhada do
  total de correspondências e da competência consultada.
- **Competência**: mês e ano de referência dos dados exibidos, exibidos em todas as telas.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: O usuário encontra a ficha completa de uma empresa conhecida em até 60
  segundos a partir da abertura da tela.
- **SC-002**: A consulta por CNPJ retorna a ficha completa em até 3 segundos para pelo menos
  95% das consultas.
- **SC-003**: Uma busca com filtros retorna a primeira página de resultados em até 5
  segundos para pelo menos 95% das buscas.
- **SC-004**: 100% dos CPFs exibidos ou presentes em mensagens de erro estão mascarados.
- **SC-005**: A competência dos dados está visível em 100% das telas que exibem dados de
  empresas.
- **SC-006**: 100% dos CPFs presentes em log de aplicação estão mascarados ou ausentes.
- **SC-007**: Em testes com razão social digitada com erro, a empresa pretendida aparece
  entre os 10 primeiros resultados em pelo menos 90% dos casos.
- **SC-008**: 90% dos respondentes de uma verificação com usuários relatam que conseguem
  responder a pergunta "qual o CNAE e em quais cidades a empresa X atua" usando apenas esta
  tela.
- **SC-009**: O usuário não encontra nenhuma etapa de credencial entre abrir a tela e
  executar a primeira consulta.

## Assumptions

- Os dados consultáveis são os mesmos já ingeridos pelo pipeline existente (empresas,
  estabelecimentos, sócios, regime de tributário e tabelas de apoio), e a tela é
  responsável por consultar, não por coletar.
- A base é consultada a partir da competência mais recente disponível, e o usuário pode
  identificar qual competência está em uso a partir da própria tela.
- Os dados da Receita Federal são públicos; a única informação de caráter pessoal no
  conjunto é o CPF do sócio, tratado como mascarado por padrão.
- O ambiente de uso é a rede interna do projeto, sem autenticação, com os dados mantidos em
  armazenamento controlado pelo próprio time.
- A feature não altera o pipeline de ingestão: ela apenas consome o resultado já
  produzido.
- A interface é web, acessível por navegador, sem necessidade de instalação de aplicativo
  pelo usuário final.
- O volume alvo é da ordem de dezenas de milhões de empresas, com busca por CNPJ e por
  filtros respondendo de forma interativa.
- Exportação de resultados e painel de agregados ficam fora desta versão, por decisão de
  escopo, e serão specs separados.
