# Modelo de Dados — Explorador Visual de Dados CNPJ

**Feature**: `001-cnpj-query-screen` · **Data**: 2026-10-27
**Fontes**: [spec.md](./spec.md) · [research.md](./research.md) · `schema.py`
(nomes canônicos das colunas) · Parquets Bronze da competência `2026-10`

---

## 1. Inventário de Origem

O inventário atual contém a competência `2026-10`. O pipeline publica um Parquet por ZIP
em `bronze/<AAAA-MM>/<nome-base>.parquet`; todos os valores são gravados como texto pelo
conversor atual.

| Família | Arquivos esperados | Uso na consulta |
|---|---|---|
| Empresas | `Empresas0.parquet` … `Empresas9.parquet` | Cadastro da pessoa jurídica |
| Estabelecimentos | `Estabelecimentos0.parquet` … `Estabelecimentos9.parquet` | Endereço, situação, CNAEs e contatos |
| Sócios | `Socios0.parquet` … `Socios9.parquet` | Quadro societário |
| Simples/MEI | `Simples.parquet` | Opção e datas de Simples/MEI |
| Apoio | `Cnaes.parquet`, `Motivos.parquet`, `Municipios.parquet`, `Naturezas.parquet`, `Paises.parquet`, `Qualificacoes.parquet` | Descrições legíveis |

**Total**: 37 arquivos. O loader MUST falhar de forma explícita se uma família obrigatória
estiver ausente, se houver mais de um `Simples.parquet` ou se as colunas não corresponderem
a `schema.py`.

### 1.1 Formatos de origem observados

| Campo | Formato | Tratamento |
|---|---|---|
| CNPJ básico | 8 dígitos | texto, chave da empresa |
| CNPJ ordem | 4 dígitos | compõe o CNPJ completo |
| CNPJ DV | 2 dígitos | compõe o CNPJ completo |
| Datas | `AAAAMMDD`, `00000000` ou vazio | data; ausência vira `NULL` |
| Capital social | texto com vírgula decimal (`5000,00`) | `NUMERIC(18,2)` |
| CNAEs secundários | string separada por vírgula; o schema menciona `;` | dividir por `,` ou `;`, remover espaços e duplicatas |
| Flags de Simples/MEI | `S`/`N` | booleano derivado |
| CPF/CNPJ de sócio | pode vir já mascarado | máscara defensiva; nunca armazenar em claro |
| UF | sigla de 2 letras | texto validado |
| Município | código de 4 dígitos | junção com a tabela de apoio |

## 2. Entidades de Domínio

### 2.1 Competência

Retrato mensal dos dados. O inventário atual tem `2026-10`; a fonte é a Receita Federal.
A competência e a fonte acompanham todas as páginas que exibem dados. Não há comparação
entre competências no MVP.

### 2.2 Empresa

Pessoa jurídica identificada pelo CNPJ básico.

| Campo lógico | Origem | Regra |
|---|---|---|
| `cnpj_basico` | `empresa_cnpj_basico` | 8 dígitos; chave |
| `razao_social` | `empresa_razao_social` | texto; exibição com redação defensiva de CPF |
| `razao_social_normalizada` | derivado | minúsculas, sem acentos/pontuação, espaços colapsados |
| `natureza_juridica_codigo` | `empresa_natureza_juridica` | junção com `naturezas_juridicas` |
| `qualificacao_responsavel_codigo` | `empresa_qualificacao_responsavel` | junção com `qualificacoes_socios` |
| `capital_social` | `empresa_capital_social` | decimal; ausência/inválido vira `NULL` |
| `porte_codigo` | `empresa_porte` | catálogo de porte |
| `ente_federativo` | `empresa_ente_federativo` | texto opcional |

**Relacionamentos**: 1 empresa → N estabelecimentos; 1 → N sócios; 1 → 0..1 Simples/MEI.

### 2.3 Estabelecimento

Endereço operacional da empresa, identificado pelo CNPJ completo.

| Campo lógico | Origem | Regra |
|---|---|---|
| `cnpj_completo` | derivado de básico + ordem + DV | 14 dígitos; chave |
| `cnpj_basico` | `estabelecimento_cnpj_basico` | FK para empresa |
| `identificador_matriz_filial` | `estabelecimento_identificador_matriz_filial` | `1` matriz, `2` filial |
| `nome_fantasia` | `estabelecimento_nome_fantasia` | opcional |
| `situacao_cadastral` | `estabelecimento_situacao_cadastral` | catálogo de situação |
| `data_situacao_cadastral` | `..._data_situacao_cadastral` | data opcional |
| `motivo_situacao_cadastral` | `..._motivo_situacao_cadastral` | junção com `motivos` |
| `data_inicio_atividade` | `..._data_inicio_atividade` | data opcional |
| `cnae_principal` | `..._cnae_fiscal_principal` | 7 dígitos; junção com `cnaes` |
| `cnaes_secundarios` | `..._cnae_fiscal_secundaria` | lista; normalizada em `estabelecimento_cnaes` |
| `tipo_logradouro`, `logradouro`, `numero`, `complemento`, `bairro` | campos `estabelecimento_*` | texto; podem ser vazios |
| `cep` | `..._cep` | texto; pode ser vazio |
| `uf` | `..._uf` | 2 letras; filtro e exibição |
| `municipio_codigo` | `..._municipio` | junção com `municipios` |
| `pais_codigo`, `nome_cidade_exterior` | `..._pais`, `..._nome_cidade_exterior` | exterior, quando aplicável |
| `ddd_1`, `telefone_1`, `ddd_2`, `telefone_2`, `ddd_fax`, `numero_fax` | campos `estabelecimento_*` | contatos opcionais da RFB |
| `correio_eletronico` | `..._correio_eletronico` | contato opcional da RFB |
| `situacao_especial`, `data_situacao_especial` | campos `estabelecimento_*` | opcionais |

**Relacionamentos**: N estabelecimentos → 1 empresa; N → 1 município; 1 → N CNAEs
(principal e secundários).

**Contatos**: são os informados no cadastro da Receita Federal. Podem estar ausentes,
desatualizados ou incompletos; não são enriquecidos nem confirmados em tempo real.

### 2.4 CNAE do estabelecimento

Uma linha por código de CNAE associado ao estabelecimento.

| Campo | Regra |
|---|---|
| `cnpj_completo` | FK para estabelecimento |
| `cnae_codigo` | 7 dígitos |
| `principal` | verdadeiro para o CNAE principal; falso para secundário |

A chave é (`cnpj_completo`, `cnae_codigo`). O filtro de CNAE consulta esta tabela, portanto
casa tanto o principal quanto qualquer secundário.

### 2.5 Sócio

Vínculo entre a empresa e uma pessoa ou entidade.

| Campo lógico | Origem | Regra |
|---|---|---|
| `cnpj_basico` | `socio_cnpj_basico` | FK para empresa |
| `identificador_socio` | `socio_identificador_socio` | código de origem |
| `nome_socio` | `socio_nome_socio` | texto; pode ser vazio para alguns tipos |
| `documento_socio_mascarado` | `socio_cnpj_cpf_socio` | sempre mascarado; nunca 11/14 dígitos em claro |
| `qualificacao_socio_codigo` | `socio_qualificacao_socio` | junção com qualificações |
| `data_entrada_sociedade` | `socio_data_entrada_sociedade` | data opcional |
| `pais_codigo` | `socio_pais` | junção com países |
| `representante_legal` | `socio_representante_legal` | documento mascarado do representante legal |
| `nome_representante` | `socio_nome_representante` | opcional |
| `qualificacao_representante_legal` | `socio_qualificacao_representante_legal` | opcional |
| `faixa_etaria` | `socio_faixa_etaria` | código de origem; não é obrigatório exibir |

**Privacidade**: o valor de `socio_cnpj_cpf_socio` não é armazenado em claro no banco de
consulta. A máscara é aplicada no loader, independentemente de o arquivo de origem já vir
mascarado. A apresentação também aplica redação defensiva a padrões de CPF em texto livre
(por exemplo, razões sociais).

### 2.6 Simples/MEI

| Campo lógico | Origem | Regra |
|---|---|---|
| `cnpj_basico` | `simples_cnpj_basico` | FK para empresa |
| `opcao_simples` | `simples_opcao_simples` | `S`/`N` |
| `data_opcao_simples` | `simples_data_opcao_simples` | data opcional |
| `data_exclusao_simples` | `simples_data_exclusao_simples` | data opcional |
| `opcao_mei` | `simples_opcao_mei` | `S`/`N` |
| `data_opcao_mei` | `simples_data_opcao_mei` | data opcional |
| `data_exclusao_mei` | `simples_data_exclusao_mei` | data opcional |
| `optante_simples` | derivado | flag `S` e sem exclusão até o fim da competência |
| `optante_mei` | derivado | flag `S` e sem exclusão até o fim da competência |

Empresa sem linha em `Simples.parquet` é tratada como não optante, sem erro.

### 2.7 Tabelas de apoio

| Tabela | Conteúdo |
|---|---|
| `cnaes` | código e descrição do CNAE |
| `municipios` | código e descrição do município |
| `naturezas_juridicas` | código e descrição |
| `qualificacoes_socios` | código e descrição |
| `motivos` | código e descrição do motivo de situação |
| `paises` | código e descrição |

### 2.8 Catálogo de domínio

Códigos oficiais sem tabela correspondente nos Parquets:

| Domínio | Códigos |
|---|---|
| Situação cadastral | `01` Nula, `02` Ativa, `03` Suspensa, `04` Inapta, `05` Ativa Não Regular, `08` Baixada |
| Porte | `00` Não informado, `01` Microempresa, `03` Empresa de Pequeno Porte, `05` Demais |
| Matriz/filial | `1` Matriz, `2` Filial |

O catálogo é versionado no projeto e coberto por teste. Se um código desconhecido aparecer,
a interface exibe o código bruto e registra a ocorrência, sem inventar um rótulo.

## 3. Modelo Físico de Consulta (PostgreSQL)

### 3.1 Metadados

`query_metadata`:

| Coluna | Tipo | Regra |
|---|---|---|
| `competencia` | `text` | `AAAA-MM`; chave |
| `fonte` | `text` | `Receita Federal` |
| `carregado_em` | `timestamptz` | instante da publicação |
| `total_empresas` | `bigint` | contagem validada |
| `total_estabelecimentos` | `bigint` | contagem validada |
| `total_socios` | `bigint` | contagem validada |
| `ativo` | `boolean` | competência exibida pela web |

### 3.2 Tabelas base

- `empresas(cnpj_basico PK, razao_social, razao_social_normalizada, natureza_juridica_codigo, qualificacao_responsavel_codigo, capital_social, porte_codigo, ente_federativo)`
- `estabelecimentos(cnpj_completo PK, cnpj_basico FK, cnpj_ordem, cnpj_dv, identificador_matriz_filial, nome_fantasia, situacao_cadastral, data_situacao_cadastral, motivo_situacao_cadastral, nome_cidade_exterior, pais_codigo, data_inicio_atividade, cnae_principal, cnae_secundaria_texto, tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio_codigo, ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, numero_fax, correio_eletronico, situacao_especial, data_situacao_especial)`
- `estabelecimento_cnaes(cnpj_completo FK, cnae_codigo, principal, PK(cnpj_completo, cnae_codigo))`
- `socios(id, cnpj_basico FK, identificador_socio, nome_socio, documento_socio_mascarado, qualificacao_socio_codigo, data_entrada_sociedade, pais_codigo, representante_legal, nome_representante, qualificacao_representante_legal, faixa_etaria)`
- `simples(cnpj_basico PK, opcao_simples, data_opcao_simples, data_exclusao_simples, opcao_mei, data_opcao_mei, data_exclusao_mei, optante_simples, optante_mei)`
- tabelas de apoio conforme §2.7 e catálogo conforme §2.8.

### 3.3 Tabela de listagem

`estabelecimentos_listagem` materializa os campos necessários à página principal e evita
joins repetidos:

| Coluna | Origem |
|---|---|
| `cnpj_completo` | estabelecimento |
| `cnpj_basico` | estabelecimento |
| `razao_social` | empresa |
| `razao_social_normalizada` | empresa |
| `nome_fantasia` | estabelecimento |
| `situacao_codigo`, `situacao_descricao` | estabelecimento + catálogo |
| `uf` | estabelecimento |
| `municipio_codigo`, `municipio_descricao` | estabelecimento + município |
| `cnae_principal_codigo`, `cnae_principal_descricao` | estabelecimento + CNAE |
| `identificador_matriz_filial`, `matriz_filial_descricao` | estabelecimento + catálogo |
| `data_inicio_atividade` | estabelecimento |
| `porte_codigo`, `porte_descricao` | empresa + catálogo |
| `optante_simples`, `optante_mei` | Simples/MEI derivado |

### 3.4 Tabelas derivadas auxiliares

- `municipios_por_uf(uf, municipio_codigo, municipio_descricao, PK(uf, municipio_codigo))`:
  municípios efetivamente presentes na listagem, para o filtro dependente de UF.
- `cnaes_presentes(cnae_codigo PK, descricao)`: CNAEs efetivamente presentes no inventário,
  para o seletor/autocomplete.
- `query_metadata` conforme §3.1.

### 3.5 Índices mínimos

| Tabela | Índice | Motivo |
|---|---|---|
| `estabelecimentos_listagem` | PK `cnpj_completo` | ordenação estável e detalhe |
| `estabelecimentos_listagem` | `(uf, municipio_codigo)` | UF + município |
| `estabelecimentos_listagem` | `(situacao_codigo)` | situação |
| `estabelecimentos_listagem` | `(porte_codigo)` | porte |
| `estabelecimentos_listagem` | `(data_inicio_atividade)` | intervalo de abertura |
| `estabelecimentos_listagem` | `(cnpj_basico)` | junção com empresa/sócios/simples |
| `estabelecimentos_listagem` | GIN `razao_social_normalizada` (`pg_trgm`) | busca literal por substring |
| `estabelecimento_cnaes` | PK `(cnpj_completo, cnae_codigo)` | junção e deduplicação |
| `estabelecimento_cnaes` | `(cnae_codigo)` | filtro por CNAE |
| `empresas` | PK `cnpj_basico` | junções |
| `socios` | `(cnpj_basico)` | detalhe |
| `simples` | PK `cnpj_basico` | junção |
| `municipios_por_uf` | PK `(uf, municipio_codigo)` | opções do filtro |
| `cnaes_presentes` | PK `cnae_codigo` | opções do filtro |

## 4. Transformações de Carga

1. **CNPJ completo**: concatenar `estabelecimento_cnpj_basico` (8) +
   `estabelecimento_cnpj_ordem` (4) + `estabelecimento_cnpj_dv` (2); validar 14 dígitos.
2. **Datas**: converter `AAAAMMDD` para `date`; `00000000`, vazio ou inválido viram `NULL`
   e incrementam um contador de descartes.
3. **Capital social**: substituir vírgula por ponto e converter para `NUMERIC(18,2)`;
   vazio vira `NULL`.
4. **CNAEs**: dividir a string secundária por `,` ou `;`, remover espaços/aspas, manter
   códigos de 7 dígitos, deduplicar e inserir em `estabelecimento_cnaes`; o principal entra
   com `principal=true`.
5. **Nome normalizado**: minúsculas, remoção de acentos (NFKD), pontuação convertida em
   espaço e espaços colapsados; usado apenas para busca literal.
6. **CPF**: aplicar máscara `***.***.***-**` em `socio_cnpj_cpf_socio`; nunca persistir o
   valor original no banco de consulta.
7. **Simples/MEI**: derivar `optante_simples` e `optante_mei` a partir da flag e da data de
   exclusão, considerando o último dia da competência.
8. **Rótulos**: juntar códigos de situação, porte, matriz/filial e tabelas de apoio;
   códigos desconhecidos preservam o valor bruto e geram alerta de qualidade.
9. **Listagem**: materializar `estabelecimentos_listagem` após as tabelas base.
10. **Validação**: conferir contagens, chaves e referências; atualizar `query_metadata`;
    publicar por troca atômica.

## 5. Regras de Validação de Entrada

| Entrada | Regra | Falha |
|---|---|---|
| CNPJ | 14 dígitos após remover máscara; dígitos verificadores válidos | 400, sem consulta |
| UF | 2 letras entre as UFs presentes | 400 |
| Município | código presente em `municipios_por_uf` para a UF | 400 |
| CNAE | 7 dígitos e presente em `cnaes_presentes` | 400 |
| Situação | código do catálogo | 400 |
| Porte | código do catálogo | 400 |
| Matriz/filial | `1` ou `2` | 400 |
| Data | `AAAA-MM-DD`; inicial ≤ final | 400 |
| Simples/MEI | `sim`/`nao`/`qualquer` | 400 |
| Página | inteiro ≥ 1 | 400 |
| Busca por nome | pelo menos 3 caracteres não vazios | 400 com orientação |
| Parâmetro repetido | um valor por filtro | 400 |

## 6. Transições de Estado

### 6.1 Carga

```text
ocioso → staging → validado → publicado
                 └→ falha → rollback (dados anteriores permanecem)
```

- A carga usa tabelas de staging e publica por troca atômica.
- Falha em qualquer etapa não deixa a competência parcialmente visível.
- Reexecutar a mesma competência substitui o retrato de forma idempotente.

### 6.2 Aplicação

A aplicação é somente leitura. Cada requisição:

```text
validação dos parâmetros → consulta → renderização
```

Não há escrita, sessão de usuário, carrinho ou estado global mutável. O estado dos filtros
vive na URL.

### 6.3 Consumidores futuros

O read model (`estabelecimentos_listagem`, tabelas base e de apoio) é reutilizável por uma
futura API para terceiros e por BI, mas esses consumidores não fazem parte do MVP. A API
deverá reutilizar as mesmas consultas/validações e ter contrato, autenticação e rate
limiting próprios; o Superset deverá usar papel somente leitura, metadados próprios e, em
produção, uma réplica de leitura/base analítica.

## 7. Referências

- [Contrato HTTP das páginas](./contracts/query-api.md)
- [Contrato dos dados](./contracts/data-contracts.md)
- [Pesquisa e decisões](./research.md)
- [Especificação](./spec.md)
- `schema.py` — nomes canônicos das colunas de origem
