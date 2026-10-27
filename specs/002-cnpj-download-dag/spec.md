# Feature Specification: Primeira DAG de Download de Dados CNPJ

**Feature Branch**: `002-cnpj-download-dag`

**Created**: 2026-10-26

**Status**: Draft

**Input**: User description: "Criar uma primeira DAG do Airflow para executar manualmente o downloader de dados CNPJ que já existe no repositório. A DAG deve receber uma competência no formato AAAA-MM, listar os arquivos dessa competência na fonte da Receita Federal e baixá-los para um diretório persistente acessível pelo container que executa a tarefa.

Reutilizar o código de extração e download existente, tornando-o chamável como função sem executar trabalho durante a importação da DAG. A tarefa deve registrar quantos arquivos foram encontrados, baixados, ignorados por já existirem e falhar se algum download falhar.

O primeiro teste deve poder limitar a quantidade de arquivos baixados, para evitar iniciar uma competência inteira sem intenção. Não adicionar agendamento automático, conversão para Parquet, publicação no R2, interface web ou API nesta etapa.

Incluir cenários para competência inválida, competência sem arquivos, arquivo já baixado e falha de download. Considerar que as tarefas rodam em containers Airflow e precisam ter acesso ao código, dependências e diretório de saída."

**Escopo desta etapa**: uma única execução manual que baixa os arquivos brutos de uma competência. Agendamento automático, conversão para Parquet, publicação em storage de objetos, interface web e API ficam **fora** desta versão.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Executar manualmente o download de uma competência (Priority: P1)

Como operador do pipeline, quero disparar manualmente uma execução informando a competência
(AAAA-MM) para obter os arquivos daquele mês no diretório persistente e saber exatamente o
que aconteceu com cada arquivo.

**Why this priority**: é a menor fatia útil do pipeline: substitui o script que roda no
import com competência fixa por uma execução parametrizada, observável e repetível. Sozinha,
já entrega valor e valida conectividade com a fonte e acesso ao diretório de saída antes de
qualquer automação.

**Independent Test**: com uma competência publicada e o diretório de saída vazio, disparar a
execução com `2026-02` e verificar os arquivos no diretório persistente e o resumo com as
contagens na execução.

**Acceptance Scenarios**:

1. **Given** uma competência válida e publicada e o diretório de saída vazio, **When** a
   execução é disparada, **Then** todos os arquivos da competência são gravados no diretório
   e o resumo informa encontrados = baixados, ignorados = 0 e falhas = 0.
2. **Given** que parte dos arquivos da competência já está no diretório, **When** a execução
   é disparada, **Then** esses arquivos não são baixados de novo, são contabilizados como
   ignorados e os demais são baixados.
3. **Given** que um dos arquivos falha no download, **When** a execução termina, **Then** a
   execução é marcada como falha e o resumo identifica o arquivo que falhou, além das
   contagens de baixados, ignorados e falhas.
4. **Given** uma execução concluída, **When** o operador consulta o resultado, **Then** o
   registro mostra a competência, o diretório de saída, as contagens e a duração.
5. **Given** a DAG carregada no Airflow, **When** ela é importada ou interpretada sem ser
   disparada, **Then** nenhuma listagem, download ou acesso à rede é executado.

---

### User Story 2 - Limitar a quantidade de arquivos no primeiro teste (Priority: P2)

Como operador, quero informar um limite máximo de arquivos para validar o fluxo completo sem
baixar uma competência inteira por engano.

**Why this priority**: protege o primeiro uso e qualquer teste exploratório do custo de
baixar gigabytes desnecessários. Depende apenas da listagem e do download já entregues pelo
P1.

**Independent Test**: disparar uma execução com limite 1 e verificar que exatamente um
arquivo foi baixado, que o total encontrado continua sendo informado e que a execução está
identificada como parcial.

**Acceptance Scenarios**:

1. **Given** uma competência com M arquivos e um limite N menor que M, **When** a execução é
   disparada, **Then** no máximo N arquivos são baixados e o resumo informa o total
   encontrado, o limite aplicado e que a execução é parcial.
2. **Given** um limite N maior ou igual ao total de arquivos, **When** a execução é
   disparada, **Then** todos os arquivos são processados e o limite não altera o resultado.
3. **Given** um limite inválido (zero, negativo ou não inteiro), **When** a execução é
   disparada, **Then** ela falha antes de listar ou baixar qualquer arquivo, com mensagem
   explicativa.
4. **Given** uma execução parcial com limite N, **When** ela é repetida com o mesmo limite,
   **Then** os arquivos já presentes entre os N selecionados são ignorados e nenhum arquivo
   fora dos N selecionados é baixado.

---

### User Story 3 - Reexecutar a mesma competência sem duplicar trabalho (Priority: P3)

Como operador, quero repetir a execução da mesma competência para continuar um download
interrompido sem baixar de novo o que já está no diretório.

**Why this priority**: downloads de competência são longos e sujeitos a interrupção; a
reexecução idempotente evita custo de rede e tempo, e é pré-requisito para retentativas
seguras.

**Independent Test**: executar duas vezes a mesma competência; a segunda execução termina
com zero downloads, todos os arquivos contabilizados como ignorados e os arquivos inalterados.

**Acceptance Scenarios**:

1. **Given** todos os arquivos da competência já presentes, **When** a execução é disparada
   novamente, **Then** nenhum byte é baixado, todos são contabilizados como ignorados e
   nenhum arquivo é modificado.
2. **Given** que uma execução anterior falhou no meio, **When** a execução é repetida,
   **Then** apenas os arquivos ausentes são baixados e os já presentes permanecem intactos.
3. **Given** um arquivo temporário deixado por uma execução interrompida, **When** a execução
   é repetida, **Then** esse arquivo não é tratado como completo e é substituído.

---

### Edge Cases

- **Competência com formato inválido** (por exemplo `2026-2`, `02-2026`, `2026-13` ou texto):
  a execução falha antes de qualquer acesso à fonte, com mensagem que mostra o formato
  esperado (AAAA-MM).
- **Competência válida mas sem arquivos** (mês ainda não publicado, competência futura ou
  diretório vazio na fonte): a execução informa explicitamente que nenhum arquivo foi
  encontrado e termina sem sucesso, para o operador não confundir com uma ingestão concluída.
- **Falha ao listar os arquivos** (fonte indisponível, tempo esgotado ou falha na obtenção do
  acesso público): a execução falha com mensagem que distingue o problema de listagem de um
  problema de download; nenhum arquivo é baixado.
- **Falha no download de um arquivo** (conexão interrompida, resposta de erro ou disco cheio):
  a execução falha, o arquivo com problema é identificado e os arquivos já baixados
  permanecem válidos.
- **Arquivo existente porém vazio** (0 byte): não é considerado baixado; deve ser baixado
  novamente.
- **Arquivo temporário de execução anterior**: não é considerado completo; deve ser
  substituído.
- **Limite maior que o total de arquivos**: todos os arquivos são baixados e o resumo indica
  que o limite não restringiu a execução.
- **Diretório de saída inexistente**: é criado antes do download; se não puder ser criado ou
  não houver permissão de escrita, a execução falha antes de baixar qualquer arquivo.
- **Disco insuficiente**: a execução falha sem deixar arquivo parcial.
- **Duas execuções simultâneas da mesma competência**: não podem corromper arquivos; um
  arquivo em escrita por uma execução não pode ser lido como completo pela outra.
- **Execução bem-sucedida com limite aplicado**: precisa ficar evidente que a competência
  não foi baixada por completo.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: A execução MUST ser disparada apenas manualmente; a DAG MUST NOT ter
  agendamento automático nesta etapa.
- **FR-002**: A execução MUST receber uma competência no formato AAAA-MM e validar tanto o
  formato quanto o mês (01 a 12) antes de qualquer acesso à fonte.
- **FR-003**: Uma competência inválida MUST encerrar a execução com falha, mensagem
  explicativa e sem listar ou baixar arquivos.
- **FR-004**: A execução MUST listar os arquivos disponíveis na fonte da Receita Federal para
  a competência informada.
- **FR-005**: A execução MUST aceitar um limite opcional de arquivos, expresso como inteiro
  positivo, que restringe quantos arquivos da competência serão baixados.
- **FR-006**: O limite MUST selecionar os N primeiros arquivos em ordem determinística e
  estável; arquivos já existentes entre os selecionados contam como ignorados e não liberam
  vaga para outro arquivo.
- **FR-007**: Um limite inválido (zero, negativo ou não inteiro) MUST encerrar a execução com
  falha antes de listar ou baixar arquivos.
- **FR-008**: Quando houver limite, o resumo MUST informar o total de arquivos encontrados, o
  limite aplicado e que a execução foi parcial.
- **FR-009**: A execução MUST gravar os arquivos em um diretório persistente, configurável
  fora do código, acessível ao ambiente que executa a tarefa e capaz de sobreviver à
  recriação do container.
- **FR-010**: O diretório de saída MUST ser criado quando não existir; a execução MUST falhar
  antes de baixar arquivos quando o diretório não puder ser criado ou não permitir escrita.
- **FR-011**: A execução MUST reutilizar a lógica existente de listagem e download como
  código chamável, sem executar listagem, download ou qualquer acesso à rede durante a
  importação da DAG.
- **FR-012**: O resumo da execução MUST registrar, no mínimo: competência, diretório de
  saída, arquivos encontrados, arquivos selecionados (quando houver limite), arquivos
  baixados, arquivos ignorados por já existirem, arquivos com falha, bytes transferidos e
  duração.
- **FR-013**: Qualquer falha de download MUST marcar a execução como falha, identificar o
  arquivo afetado e preservar os arquivos já baixados com sucesso.
- **FR-014**: Arquivo já presente e não vazio MUST ser ignorado, contabilizado e MUST NOT ser
  sobrescrito nem baixado novamente.
- **FR-015**: A gravação MUST ser atômica por arquivo; falha ou interrupção MUST NOT deixar
  arquivo parcial nem arquivo temporário utilizável como se fosse completo.
- **FR-016**: Competência válida sem nenhum arquivo na fonte MUST encerrar a execução sem
  sucesso, com mensagem explícita de que nenhum arquivo foi encontrado.
- **FR-017**: Reexecutar a mesma competência MUST ser idempotente: arquivos já presentes são
  ignorados e somente os ausentes são baixados.
- **FR-018**: Execuções simultâneas da mesma competência MUST NOT corromper arquivos; cada
  arquivo MUST ser tratado de forma atômica e independente.
- **FR-019**: O log MUST NOT expor o token de acesso à fonte nem credenciais; nomes de
  arquivo, contagens e duração podem ser registrados.
- **FR-020**: A execução MUST NOT realizar, nesta etapa, conversão para Parquet, publicação
  em storage de objetos, agendamento automático, exposição web ou API.

### Key Entities

- **Competência**: mês e ano de referência no formato AAAA-MM; identificador primário de uma
  execução e chave de idempotência.
- **Execução de Download**: registro de uma tentativa manual, com competência, limite
  aplicado, diretório de saída, status final, contagens e duração.
- **Arquivo da RFB**: arquivo publicado pela Receita Federal para uma competência, com nome,
  origem, tamanho e situação (baixado, ignorado ou falhou).
- **Diretório de Saída**: local persistente configurável onde os arquivos brutos da
  competência são mantidos entre execuções e recriações de container.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% das execuções exibem, ao final, as contagens de arquivos encontrados,
  baixados, ignorados e com falha.
- **SC-002**: 0 arquivos parciais ou temporários permanecem no diretório de saída após
  qualquer falha ou interrupção.
- **SC-003**: Reexecutar uma competência já completa resulta em 0 bytes baixados e 100% dos
  arquivos contabilizados como ignorados.
- **SC-004**: Em 100% das execuções com limite N, no máximo N arquivos são baixados.
- **SC-005**: Competência inválida falha sem executar nenhuma requisição à fonte.
- **SC-006**: 100% das falhas de download resultam em execução com status de falha e com o
  arquivo afetado identificado.
- **SC-007**: Importar a DAG sem disparar a execução resulta em 0 acessos à rede e 0
  downloads.
- **SC-008**: 0 ocorrências do token de acesso à fonte aparecem no log da execução.
- **SC-009**: Em uma execução parcial por limite, o operador identifica no resumo que a
  competência não foi baixada por completo.

## Assumptions

- A fonte é o WebDAV público da Receita Federal já utilizado pelo código existente; o acesso
  e o layout dos diretórios por competência permanecem os mesmos.
- Competências são publicadas mensalmente; `2026-02` é uma competência publicada, usada como
  referência de teste.
- O ambiente que executa a tarefa tem acesso de rede à fonte e acesso de escrita ao
  diretório persistente; código e dependências já estão disponíveis nesse ambiente (stack
  Airflow provisionada no repositório).
- O diretório de saída é configurado por variável de ambiente ou configuração versionada,
  nunca hardcoded, e aponta para um volume persistente.
- O refactor dos módulos existentes de listagem e download é permitido para torná-los
  importáveis e chamáveis, preservando o comportamento atual (Princípio I da constituição).
- "Já baixado" significa arquivo existente e com tamanho maior que zero; a fonte não oferece
  verificação de integridade, então tamanho e conclusão atômica são os critérios adotados.
- Competência válida sem arquivos é tratada como falha, e não como sucesso vazio, porque a
  execução manual existe para ingerir dados e um resultado vazio indica competência errada,
  ainda não publicada ou problema na fonte.
- A execução é operada manualmente por um membro do time; a autenticação é a do próprio
  Airflow.
- Os arquivos brutos permanecem no diretório de saída nesta etapa; conversão, publicação e
  consumo ficam para specs posteriores.
- O volume de uma competência pode chegar a gigabytes; o limite de arquivos existe para
  permitir testes baratos, não para produção.
