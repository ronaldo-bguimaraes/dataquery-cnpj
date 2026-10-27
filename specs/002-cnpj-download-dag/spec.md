# Feature Specification: Download e conversão CNPJ com Airflow

**Feature Branch**: `002-cnpj-download-dag`
**Created**: 2026-10-26
**Status**: Draft
**Input**: Operar manualmente uma competência da Receita Federal com uma única DAG: baixar ou reutilizar cada ZIP em Raw no armazenamento configurado e, em seguida, converter o mesmo arquivo para Parquet em Bronze, com tarefas independentes por arquivo.

## Escopo

Há uma única DAG manual (`cnpj_download_manual`), disparada uma vez por competência
`AAAA-MM`. Não há uma DAG por arquivo nem agendamento automático.

Em runtime, a DAG lista os arquivos publicados, ordena-os de forma determinística, aplica o
limite opcional e cria, para cada ZIP selecionado, duas tarefas encadeadas:

1. `baixar_raw`: baixa o ZIP da RFB para o armazenamento configurado ou reutiliza a cópia
   Raw já existente.
2. `converter_bronze`: lê o ZIP do Raw, converte o CSV para Parquet e grava o resultado em
   Bronze.

Os ZIPs permanecem no armazenamento configurado em `raw/<AAAA-MM>/<nome-original>`. Os
Parquets ficam em `bronze/<AAAA-MM>/<nome-base>.parquet`. As cópias Raw são preservadas por
competência.

No modo local, o armazenamento é o filesystem do repositório: a pasta `data/` na raiz,
montada como `/data` nos containers que executam tarefas. Em produção, o backend pode ser
selecionado por configuração para Cloudflare R2 via API compatível com S3. Esta entrega não
inclui camadas Silver/Gold, agendamento automático, frontend ou API.

## User Scenarios & Testing

### User Story 1 — Baixar e converter uma competência (Priority: P1)

Como operador, quero disparar manualmente uma competência e obter seus ZIPs originais em Raw
e seus Parquets em Bronze, acompanhando o resultado por arquivo.

**Why this priority**: entrega o primeiro fluxo utilizável do pipeline, da fonte ao formato
analítico, mantendo os estágios rastreáveis e reprocessáveis.

**Independent Test**: executar com uma amostra de competência e confirmar, para cada ZIP
selecionado, um artefato Raw, um Parquet legível em Bronze, as colunas do schema central e a
contagem de linhas correta.

**Acceptance Scenarios**:

1. **Given** uma competência válida publicada, **When** a DAG é disparada, **Then** cada ZIP
   selecionado recebe um par de tarefas `baixar_raw → converter_bronze` e o resumo informa
   estados, bytes, linhas e localizações de saída.
2. **Given** que um download falha, **When** o run continua, **Then** `converter_bronze`
   daquele arquivo não é executado, outras instâncias independentes podem concluir e o run
   termina como falha.
3. **Given** que uma conversão falha, **When** a tarefa termina, **Then** o ZIP Raw permanece,
   nenhum Parquet parcial fica visível e outras conversões continuam independentemente.
4. **Given** que a DAG é importada pelo parser, **When** não há trigger, **Then** nenhuma
   conexão de rede, listagem, download ou conversão ocorre.

### User Story 2 — Limitar os arquivos do primeiro teste (Priority: P2)

Como operador, quero limitar o número de arquivos selecionados para validar o fluxo sem
processar uma competência inteira por engano.

**Independent Test**: disparar com limite 1 e confirmar que somente o primeiro arquivo da
ordem estável ganha as tarefas de download e conversão.

**Acceptance Scenarios**:

1. **Given** M arquivos listados e limite N menor que M, **When** o run é disparado, **Then**
   exatamente N arquivos são selecionados e o resumo identifica a execução como parcial.
2. **Given** que um arquivo selecionado já existe em Raw, **When** o run é disparado com
   limite N, **Then** esse arquivo não é baixado novamente, conta dentro dos N e ainda segue
   para conversão.
3. **Given** um limite inválido, **When** o trigger é validado, **Then** o run falha antes de
   acessar a RFB ou o armazenamento.

### User Story 3 — Reexecutar sem baixar Raw novamente (Priority: P3)

Como operador, quero repetir ou retentar o processamento sem transferir de novo ZIPs
preservados em Raw.

**Independent Test**: executar duas vezes o mesmo conjunto e confirmar que a segunda execução
não baixa bytes Raw, mas pode reconstruir Bronze.

**Acceptance Scenarios**:

1. **Given** que o ZIP Raw já existe e é válido, **When** a tarefa de download é repetida,
   **Then** ela retorna a localização existente sem requisitar o conteúdo novamente à RFB.
2. **Given** que Bronze está ausente ou a conversão anterior falhou, **When** o processamento
   é repetido, **Then** o Parquet é gerado do Raw sem nova requisição de conteúdo à RFB.
3. **Given** que existe um Parquet publicado, **When** a conversão é repetida, **Then** a
   saída é substituída somente após a nova conversão terminar com sucesso.

## Edge Cases

- Competência inválida ou limite zero/negativo/não inteiro: falha antes de qualquer requisição.
- Competência válida sem arquivos publicados: falha explícita como conjunto vazio.
- RFB indisponível: erro de listagem sanitizado, sem token nos logs; nenhum arquivo é baixado.
- Armazenamento local indisponível ou backend configurado inacessível: falha antes de iniciar
  transferências.
- ZIP Raw existente vazio ou corrompido: não é aceito como válido; deve ser baixado novamente
  ou falhar de forma explícita.
- ZIP com mais de um membro, nome sem schema conhecido ou encoding/layout inválido: falha
  apenas da instância daquele arquivo.
- Falha de disco temporário, gravação interrompida ou processo encerrado: nenhum artefato
  parcial aparece na localização final; temporários locais são limpos.
- Duas execuções da mesma competência: não podem sobrescrever artefatos parcialmente nem
  competir pela mesma localização em escrita.
- Execução com limite: o operador deve identificar que a competência não foi processada por
  completo.

### Tratamento de falhas por arquivo

| Falha | Efeito naquele arquivo | Efeito nos demais | Estado do run |
|---|---|---|---|
| Listagem da competência | Nenhum arquivo é mapeado | Nenhum arquivo é processado | failed |
| Download do ZIP | Nenhum artefato Raw válido é publicado; `converter_bronze` não executa | Outros arquivos continuam | failed |
| Gravação Raw interrompida | Nenhum artefato parcial na localização final; temporário local removido | Outros arquivos continuam | failed |
| Conversão do CSV/Parquet | Raw é preservado; nenhum Bronze parcial é publicado | Outras conversões continuam | failed |
| ZIP inválido ou schema desconhecido | Apenas a conversão daquele arquivo falha | Outras conversões continuam | failed |
| Armazenamento inacessível antes do processamento | Nenhum arquivo é processado | Nenhum arquivo é processado | failed |

Uma falha nunca é contabilizada como sucesso. Retentativas são por tarefa/arquivo, sem exigir
novo download quando o Raw já está íntegro.

## Requirements

### Functional Requirements

- **FR-001**: A feature MUST ter uma única DAG manual (`schedule=None`), executada por
  competência, sem agendamento automático e sem uma DAG por arquivo.
- **FR-002**: O trigger MUST aceitar competência `AAAA-MM` com mês 01–12 e limite opcional
  inteiro positivo; ambos MUST ser validados antes de acessar a RFB ou o armazenamento.
- **FR-003**: A execução MUST listar os arquivos da competência na RFB, filtrar diretórios e
  ordená-los de forma determinística e estável.
- **FR-004**: O limite MUST selecionar os N primeiros arquivos em ordem estável **antes** de
  verificar a presença em Raw; um artefato Raw existente conta dentro dos N e não libera vaga
  para o próximo arquivo.
- **FR-005**: Para cada ZIP selecionado, o Airflow MUST criar um par de tarefas encadeadas
  `baixar_raw → converter_bronze`, com estado e retry independentes por arquivo.
- **FR-006**: `baixar_raw` MUST gravar o ZIP no armazenamento configurado, na localização
  `raw/<AAAA-MM>/<nome-original>`, e MUST preservá-lo por competência.
- **FR-007**: Um ZIP Raw existente e não vazio MUST ser reutilizado sem novo download; o
  arquivo correspondente MUST seguir para `converter_bronze`.
- **FR-008**: `converter_bronze` MUST ler o ZIP do Raw, usar o schema central, encoding
  `latin1`, delimitador `;`, processamento em blocos e preservar os valores como texto.
- **FR-009**: Cada ZIP MUST gerar exatamente um Parquet em
  `bronze/<AAAA-MM>/<nome-base>.parquet`, no mesmo armazenamento configurado.
- **FR-010**: Uma falha no download MUST impedir a conversão daquele arquivo; uma falha de
  conversão MUST preservar o Raw e não publicar Bronze parcial. Arquivos independentes MUST
  poder continuar.
- **FR-011**: Se qualquer instância de arquivo falhar, o run MUST terminar como failed,
  preservando os artefatos concluídos com sucesso e permitindo retry por arquivo.
- **FR-012**: A publicação de Raw e Bronze MUST ser atômica na localização final do backend
  configurado; falha ou interrupção MUST NOT expor artefato incompleto nem substituir uma
  saída válida por uma incompleta.
- **FR-013**: Arquivos intermediários MUST ficar em área temporária local da tarefa e ser
  removidos ao término; XCom MUST transportar apenas localizações lógicas e metadados
  pequenos, nunca conteúdo de arquivo.
- **FR-014**: O resumo final MUST registrar competência, encontrados, selecionados, parcial,
  estados por arquivo, bytes, duração, linhas processadas e localizações Raw/Bronze, sem
  URL/token/credenciais.
- **FR-015**: Importar ou parsear a DAG MUST NOT executar chamadas de rede, leitura de dados
  ou conversão.
- **FR-016**: A seleção do backend de armazenamento e seus parâmetros (caminho base, endpoint,
  bucket e credenciais, quando aplicável) MUST vir de configuração disponível em todos os
  containers Airflow e MUST NOT estar hardcoded. No modo local, o caminho base MUST ser
  `/data`; em produção, a configuração MUST permitir selecionar Cloudflare R2 via API
  compatível com S3.
- **FR-017**: Esta feature MUST NOT adicionar camadas Silver/Gold, schedule automático,
  frontend ou API. O provisionamento operacional do R2 e a migração de dados para ele ficam
  fora do escopo; a seleção do backend R2 por configuração S3-compatível faz parte do escopo.
- **FR-018**: `baixar_raw` e `converter_bronze` MUST acessar o armazenamento exclusivamente
  por uma interface genérica, com operações de verificação, leitura, escrita atômica e
  remoção, sem depender diretamente de filesystem, bucket ou provedor.
- **FR-019**: No modo local, o backend de filesystem MUST usar `data/` na raiz do repositório,
  montado como `/data` nos containers que executam tarefas, preservando
  `raw/<AAAA-MM>/` e `bronze/<AAAA-MM>/`.
- **FR-020**: Em produção, a seleção do backend MUST permitir Cloudflare R2 por meio de API
  compatível com S3 e configuração externa, sem alterar a DAG, o download ou a conversão.

```text
DAG manual (uma por execução de competência)
└── por ZIP selecionado: baixar_raw → converter_bronze
    ├── baixar_raw:      raw/<AAAA-MM>/<nome-original>
    └── converter_bronze: bronze/<AAAA-MM>/<nome-base>.parquet
```

### Key Entities

- **Competência**: mês/ano `AAAA-MM`, chave da execução.
- **Arquivo da RFB**: ZIP selecionado, com nome e localização remota sanitizada.
- **Backend de Armazenamento**: implementação da interface genérica selecionada por
  configuração; filesystem local (`data/`/`/data`) ou Cloudflare R2 via API S3.
- **Artefato Raw**: ZIP original preservado sob `raw/<AAAA-MM>/<nome-original>` no backend
  configurado.
- **Artefato Bronze**: Parquet gerado do ZIP, sob `bronze/<AAAA-MM>/<nome-base>.parquet` no
  backend configurado.
- **Instância de Arquivo**: par de tarefas `baixar_raw → converter_bronze` de um ZIP.
- **Resumo do Run**: metadados e estados das instâncias, guardados no Airflow; sem conteúdo
  de arquivos.

## Success Criteria

### Measurable Outcomes

- **SC-001**: 100% dos ZIPs selecionados exibem, no Airflow, as duas tarefas
  `baixar_raw` e `converter_bronze`, com a segunda dependendo da primeira.
- **SC-002**: 100% dos Parquets publicados são legíveis, correspondem ao schema do conjunto
  e têm contagem de linhas igual à soma dos blocos processados.
- **SC-003**: Em 100% das falhas de download, a conversão daquele arquivo não é executada e
  as instâncias independentes ainda podem concluir; o run termina failed.
- **SC-004**: Reexecução com Raw existente transfere zero bytes de conteúdo da RFB para esses
  arquivos e ainda permite reconstruir Bronze.
- **SC-005**: Em todo uso de limite N, no máximo N grupos de tarefas por arquivo são criados;
  um Raw existente entre os N continua contando e é convertido.
- **SC-006**: Nenhum artefato final parcial ou temporário local permanece após sucesso ou
  falha tratada.
- **SC-007**: Importar a DAG não faz I/O externo.
- **SC-008**: Tokens e credenciais não aparecem em logs, erros ou XCom.
- **SC-009**: Execução parcial por limite é identificada claramente no resumo.
- **SC-010**: Toda falha por arquivo identifica o nome do arquivo e a etapa afetada (download
  ou conversão).
- **SC-011**: Em modo local, 100% dos artefatos Raw e Bronze são encontrados sob
  `data/raw/<AAAA-MM>/` e `data/bronze/<AAAA-MM>/`, com os mesmos nomes e conteúdo do fluxo
  validado.
- **SC-012**: A troca do backend local pelo backend de produção ocorre apenas por
  configuração, sem alterar DAG, download ou conversão e sem caminhos ou credenciais
  hardcoded.
- **SC-013**: Em produção, 100% das execuções configuradas para Cloudflare R2 usam a API
  compatível com S3, sem alteração no fluxo de download/conversão.

## Assumptions

- No modo local, `data/` na raiz do repositório existe e é gravável; os containers que
  executam tarefas o montam como `/data` e compartilham o mesmo conteúdo.
- Em produção, o backend Cloudflare R2 é configurado por endpoint, bucket e credenciais
  compatíveis com S3; o provisionamento desses recursos é operacional.
- O backend configurado expõe os prefixos lógicos `raw/` e `bronze/`.
- Raw é retido por competência, conforme a constituição 1.1.0; política de expiração será
  definida separadamente.
- "Raw existente válido" significa artefato presente e com tamanho maior que zero; a RFB não
  oferece verificação de integridade, então presença e publicação atômica são os critérios.
- O limite se aplica ao conjunto antes de verificar Raw existente; cada selecionado existente
  ainda é convertido.
- Cada ZIP contém exatamente um membro CSV; layouts diferentes falham apenas na conversão
  daquele arquivo.
- Airflow controla o paralelismo real das instâncias mapeadas pelos limites globais do
  executor; a DAG não adiciona um cap próprio.
- A conversão inicial só normaliza formato para Parquet e nomes de colunas do schema;
  tipagem/limpeza pertencem à camada Silver futura.
- O primeiro teste usa limite 1; competência completa pode consumir muitos recursos e não é o
  teste inicial.
