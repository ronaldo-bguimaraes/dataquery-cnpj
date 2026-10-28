# Quickstart — Explorador Visual de Dados CNPJ

**Feature**: `001-cnpj-query-screen` · **Data**: 2026-10-27
**Objetivo**: validar ponta a ponta a listagem, os filtros, a paginação e os detalhes da
competência `2026-10`, sem exportação/download e com CPF mascarado.

Este guia descreve **como validar**; a implementação e as tarefas ficam em
[plan.md](./plan.md) e `tasks.md`. Contratos e modelo estão em
[contracts/](./contracts/) e [data-model.md](./data-model.md).

---

## 1. Pré-requisitos

| Item | Verificação |
|---|---|
| Docker + Docker Compose v2 | `docker compose version` |
| `.env` na raiz, criado a partir de `.env.example` | `test -f .env` |
| Parquets Bronze da competência `2026-10` | `find data/bronze/2026-10 -maxdepth 1 -name '*.parquet' | wc -l` → `37` |
| Pipeline Raw → Bronze já executado para `2026-10` | arquivos `Empresas*`, `Estabelecimentos*`, `Socios*`, `Simples` e apoio presentes |

O portal usa PostgreSQL 16 em um serviço próprio (`query-db`); ele **não** compartilha o
banco de metadados do Airflow. A web não acessa os Parquets nem o bucket; somente o loader
faz isso.

### Variáveis principais

```dotenv
CNPJ_QUERY_COMPETENCIA=2026-10
CNPJ_QUERY_DATABASE_URL=postgresql://cnpj_query_app:...@query-db:5432/cnpj_query
CNPJ_QUERY_PAGE_SIZE=25
CNPJ_STORAGE_BACKEND=filesystem
CNPJ_DATA_ROOT=/data
```

Para o modo S3/R2, o loader usa as mesmas variáveis `CNPJ_S3_*` do pipeline. A web nunca
recebe credenciais de storage, apenas a URL do banco com papel somente leitura.

---

## 2. Subir o banco e carregar a competência

```shell
docker compose --profile query build query-web query-loader
docker compose --profile query up -d query-db
docker compose --profile query run --rm query-loader
```

O loader deve:

1. descobrir as famílias de Parquets da competência;
2. validar o schema de origem;
3. carregar as tabelas de staging;
4. derivar CNPJ completo, datas, CNAEs, nome normalizado, CPF mascarado e optantes;
5. construir a listagem e os índices;
6. validar contagens/referências;
7. publicar `query_metadata` e trocar as tabelas de forma atômica.

Saída esperada: log com competência, contagem por família, duração, memória e
`publicado`. Uma falha deve deixar a competência anterior intacta e retornar código de erro
diferente de zero.

---

## 3. Subir a aplicação

```shell
docker compose --profile query up -d query-web
docker compose ps
```

Esperado: `query-db` e `query-web` saudáveis. Abra:

```text
http://localhost:8000
```

A aplicação deve redirecionar para `/estabelecimentos`.

---

## 4. Validar o contrato HTTP

### 4.1 Saúde

```shell
curl -s http://localhost:8000/health | jq .
```

Esperado:

```json
{
  "status": "ok",
  "competencia": "2026-10",
  "fonte": "Receita Federal",
  "banco": "ok"
}
```

### 4.2 Competência e fonte

Abra `/estabelecimentos` e confirme, no topo da página:

- competência `2026-10`;
- fonte `Receita Federal`;
- aviso de que o dado é um retrato mensal, não tempo real;
- nenhuma opção de comparação histórica ou troca de competência.

### 4.3 Filtros combinados

Na interface, aplique uma combinação como:

| Filtro | Exemplo |
|---|---|
| UF | `SP` |
| Situação | `02` (Ativa) |
| CNAE | um código de 7 dígitos existente |
| Porte | `01`, `03` ou `05` |
| Matriz/filial | `1` ou `2` |
| Data de abertura | intervalo válido |
| Simples/MEI | `sim`/`nao`/`qualquer` |

Esperado:

- a lista contém somente estabelecimentos que atendem a **todos** os filtros;
- a contagem total é exibida e é maior ou igual ao número de linhas da página;
- o CNAE casa principal **ou** secundário;
- a página atual é indicada;
- os filtros permanecem preenchidos.

### 4.4 Paginação

1. Aplique filtros que retornem mais de 25 registros.
2. Navegue para a próxima página.
3. Volte para a anterior.

Esperado: os filtros permanecem, não há registros repetidos/omitidos e a página atual é
indicada. Alterar um filtro deve voltar para a página `1`.

### 4.5 Estado vazio

Use uma combinação válida sem correspondências (por exemplo, uma UF com um CNAE improvável).

Esperado:

- contagem `0`;
- mensagem clara;
- ação para limpar os filtros;
- nenhum erro HTTP.

### 4.6 Validações

| Entrada | Esperado |
|---|---|
| CNPJ com letra ou dígito verificador inválido | mensagem explicativa, sem consulta |
| CNPJ válido inexistente | página de não encontrado / lista vazia, sem erro interno |
| Data inicial > data final | mensagem explicativa, sem consulta |
| Município incompatível com a UF | ajuste/validação, sem filtro impossível |
| CNAE com formato inválido | mensagem explicativa |
| Busca por nome com menos de 3 caracteres | orientação para ampliar o termo |
| Busca com erro de digitação | resultado vazio; **não** há correção automática |

### 4.7 Detalhe do estabelecimento

Clique em um resultado e confirme:

- CNPJ completo, matriz/filial, situação e data da situação;
- nome fantasia, quando houver;
- endereço completo, UF e município;
- data de abertura;
- CNAE principal e secundários;
- contatos cadastrais, quando disponíveis, identificados como dados da Receita Federal e
  sujeitos a ausência/desatualização;
- dados da empresa: razão social, natureza jurídica, porte, capital social, qualificação do
  responsável e ente federativo, quando aplicável;
- Simples/MEI com datas ou indicação de ausência;
- sócios, quando houver, com documento sempre mascarado;
- link de retorno que preserva filtros e página.

### 4.8 Privacidade e escopo

- Nenhum CPF aparece com 11 dígitos em tela, mensagem de erro ou log.
- Nenhuma tela oferece botão/link de exportação, download ou CSV.
- Nenhuma tela exibe Lucro Real/Presumido/Arbitrado, PGFN, CNO ou contato enriquecido.
- Nenhuma tela exibe comparação histórica.

### 4.9 Teste de usabilidade (SC-004)

Recrute participantes **sem perfil predefinido** (não presuma analista, contador ou
desenvolvedor). Peça que realizem, sem ajuda:

1. filtrar por UF e CNAE;
2. abrir um estabelecimento;
3. identificar a situação cadastral e a opção pelo Simples/MEI.

Registre sucesso na primeira tentativa. Meta: **≥ 90%** dos participantes concluem a tarefa
sem ajuda. O roteiro e o resultado devem ser anexados à validação da feature.

Consultas SQL de auditoria (execute no banco de consulta):

```sql
-- CPF em claro não deve existir
SELECT count(*) FROM socios
WHERE documento_socio_mascarado ~ '^[0-9]{11}$';

-- Metadados da competência ativa
SELECT competencia, fonte, total_empresas, total_estabelecimentos, total_socios
FROM query_metadata WHERE ativo;
```

Esperado: `0` para a primeira consulta e a competência `2026-10` na segunda.

---

## 5. Executar a suíte

```shell
docker compose --profile query-test run --rm query-tests pytest tests/query -q
```

Cobertura esperada:

- unitários: normalização, datas, capital, CNAEs, máscara de CPF, optantes, validação;
- integração: carga em PostgreSQL, contagem, filtros, paginação, detalhe e retorno;
- contrato: páginas, parâmetros, estados de erro e ausência de exportação/download;
- E2E: fluxo listagem → detalhe → retorno com filtros preservados.

E2E opcional:

```shell
docker compose --profile query-test run --rm query-tests \
  pytest tests/query/e2e -q
```

---

## 6. Benchmark

Com a competência completa carregada:

```shell
docker compose --profile query run --rm query-web \
  python -m src.query.benchmark --requisicoes 50
```

Registre no README/plano:

| Métrica | Alvo |
|---|---|
| `GET /estabelecimentos` filtrada — p95 | ≤ 5 s |
| `GET /estabelecimentos/{cnpj}` — p95 | ≤ 3 s |
| Carga da competência — duração e memória de pico | registrar |
| Contagem sem filtros | usar `query_metadata` |
| Contagem filtrada | `COUNT(*)` indexado |

Se o alvo não for atingido, ajustar índices/consultas e repetir; não adicionar motor de
busca ou cache distribuído sem uma decisão registrada.

---

## 7. Encerrar

```shell
docker compose down
```

Para remover também o volume do banco de consulta (não afeta `data/`):

```shell
docker compose down -v
```

---

## 8. Critérios de aceite da validação

- [ ] Competência `2026-10` e fonte `Receita Federal` visíveis em todas as páginas de dados.
- [ ] Filtros combináveis funcionam e atualizam contagem/lista.
- [ ] CNAE filtra principal e secundário.
- [ ] Paginação preserva filtros e não repete/omite registros.
- [ ] Estado vazio e validações são claros.
- [ ] Detalhe exibe estabelecimento, empresa, sócios e Simples/MEI.
- [ ] Retorno do detalhe preserva filtros e página.
- [ ] CPF sempre mascarado; nenhum CPF completo em log.
- [ ] Nenhuma exportação/download/CSV.
- [ ] Nenhum dado de fonte externa ou comparação histórica.
- [ ] ≥ 90% dos participantes do teste de usabilidade concluem a tarefa principal.
- [ ] p95 dentro dos alvos e carga registrada.

---

## 9. Evolução futura

API pública e dashboards de BI não são validados neste quickstart. A arquitetura mantém o
PostgreSQL e o read model desacoplados para que esses consumidores possam ser adicionados
em features próprias; consulte “Future Evolution” em [plan.md](./plan.md).

## 10. Referências

- [Especificação](./spec.md)
- [Plano](./plan.md)
- [Pesquisa](./research.md)
- [Modelo de dados](./data-model.md)
- [Contratos](./contracts/)
- [Checklist de requisitos](./checklists/requirements.md)
