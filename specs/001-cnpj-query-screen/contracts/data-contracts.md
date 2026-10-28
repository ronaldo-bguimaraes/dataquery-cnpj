# Contrato dos Dados — Parquets de Origem e Schema de Consulta

**Feature**: `001-cnpj-query-screen` · **Data**: 2026-10-27
**Escopo**: contrato dos Parquets Bronze consumidos pelo loader e do schema PostgreSQL
publicado para a aplicação web.
**Fonte de verdade dos nomes de coluna**: `schema.py` (Princípio II da constituição).

---

## 1. Parquets de Origem

### 1.1 Localização e nomenclatura

```text
<data-root>/bronze/<AAAA-MM>/<nome-base>.parquet
```

Exemplos para a competência `2026-10`:

```text
data/bronze/2026-10/Empresas0.parquet
data/bronze/2026-10/Estabelecimentos0.parquet
data/bronze/2026-10/Socios0.parquet
data/bronze/2026-10/Simples.parquet
data/bronze/2026-10/Cnaes.parquet
```

| Família | Padrão | Quantidade esperada em `2026-10` |
|---|---|---|
| Empresas | `Empresas*.parquet` | 10 |
| Estabelecimentos | `Estabelecimentos*.parquet` | 10 |
| Sócios | `Socios*.parquet` | 10 |
| Simples/MEI | `Simples.parquet` | 1 |
| Apoio | `Cnaes.parquet`, `Motivos.parquet`, `Municipios.parquet`, `Naturezas.parquet`, `Paises.parquet`, `Qualificacoes.parquet` | 6 |

**Total**: 37 arquivos. O loader MUST falhar se uma família obrigatória estiver ausente,
se houver mais de um `Simples.parquet` ou se as colunas não corresponderem a `schema.py`.

### 1.2 Tipos e formatos

O conversor atual grava todas as colunas como texto. O loader é responsável por interpretar
os valores.

| Campo de origem | Formato | Conversão |
|---|---|---|
| `empresa_cnpj_basico` | 8 dígitos | texto; chave |
| `estabelecimento_cnpj_basico` | 8 dígitos | texto; FK |
| `estabelecimento_cnpj_ordem` | 4 dígitos | texto; compõe CNPJ |
| `estabelecimento_cnpj_dv` | 2 dígitos | texto; compõe CNPJ |
| Datas (`*_data_*`) | `AAAAMMDD` | `date`; `00000000`/vazio → `NULL` |
| `empresa_capital_social` | decimal com vírgula | `NUMERIC(18,2)`; vazio → `NULL` |
| `estabelecimento_cnae_fiscal_secundaria` | códigos separados por vírgula ou `;` | normalizar em `estabelecimento_cnaes` |
| `simples_opcao_simples`, `simples_opcao_mei` | `S`/`N` | booleano de opção |
| `socio_cnpj_cpf_socio` | CPF/CNPJ, possivelmente já mascarado | sempre mascarado no banco de consulta |
| `socio_representante_legal` | documento do representante, possivelmente mascarado | sempre mascarado no banco de consulta |
| `estabelecimento_uf` | 2 letras | texto |
| `estabelecimento_municipio` | 4 dígitos | texto; FK para município |
| `estabelecimento_pais`, `socio_pais` | 3 dígitos | texto; FK para país |
| `estabelecimento_cnae_fiscal_principal` | 7 dígitos | texto; FK para CNAE |

### 1.3 Colunas canônicas

As colunas são exatamente as de `schema.py`:

- `empresas`: 7 colunas (`empresa_*`);
- `estabelecimentos`: 30 colunas (`estabelecimento_*`);
- `socios`: 11 colunas (`socio_*`);
- `simples`: 7 colunas (`simples_*`);
- tabelas de apoio: `paises` (2), `municipios` (2), `qualificacoes_socios` (2),
  `naturezas_juridicas` (2), `cnaes` (2), `motivos` (2).

O loader MUST validar a presença e a ordem/nome das colunas antes de carregar. O schema de
origem é imutável dentro da competência.

---

## 2. Transformações de Carga

As regras detalhadas estão em [../data-model.md](../data-model.md). Resumo:

1. **CNPJ completo**: básico (8) + ordem (4) + DV (2) = 14 dígitos.
2. **Datas**: `AAAAMMDD` → `date`; ausência/inválido → `NULL` com contador.
3. **Capital social**: vírgula decimal → `NUMERIC(18,2)`.
4. **CNAEs**: dividir por `,` ou `;`, limpar, deduplicar, validar 7 dígitos; principal e
   secundários em `estabelecimento_cnaes`.
5. **Nome normalizado**: minúsculas, sem acentos, pontuação como espaço, espaços
   colapsados; usado somente para busca literal.
6. **CPF**: máscara `***.***.***-**`; o valor original não é persistido no banco de
   consulta.
7. **Simples/MEI**: `optante_*` = flag `S` e sem exclusão até o último dia da competência.
8. **Rótulos**: situação, porte e matriz/filial vêm do catálogo de domínio; os demais vêm
   das tabelas de apoio.
9. **Listagem**: materializar `estabelecimentos_listagem` após as tabelas base.
10. **Publicação**: validar contagens/referências e trocar as tabelas de staging de forma
    atômica.

---

## 3. Schema PostgreSQL de Consulta

### 3.1 Metadados

`query_metadata`:

| Coluna | Tipo | Regra |
|---|---|---|
| `competencia` | `text` | `AAAA-MM`; PK |
| `fonte` | `text` | `Receita Federal` |
| `carregado_em` | `timestamptz` | instante da publicação |
| `total_empresas` | `bigint` | contagem validada |
| `total_estabelecimentos` | `bigint` | contagem validada |
| `total_socios` | `bigint` | contagem validada |
| `ativo` | `boolean` | competência exibida |

### 3.2 Tabelas base

- `empresas`
- `estabelecimentos`
- `estabelecimento_cnaes`
- `socios`
- `simples`
- `cnaes`
- `municipios`
- `naturezas_juridicas`
- `qualificacoes_socios`
- `motivos`
- `paises`
- `situacoes_cadastrais`
- `portes`
- `matriz_filial`
- `municipios_por_uf`
- `cnaes_presentes`
- `estabelecimentos_listagem`

As colunas e relacionamentos estão em [../data-model.md](../data-model.md) §3. As chaves
mínimas são:

| Tabela | Chave |
|---|---|
| `empresas` | `cnpj_basico` |
| `estabelecimentos` | `cnpj_completo` |
| `estabelecimento_cnaes` | (`cnpj_completo`, `cnae_codigo`) |
| `socios` | `id`; índice por `cnpj_basico` |
| `simples` | `cnpj_basico` |
| `cnaes` | `codigo` |
| `municipios` | `codigo` |
| `naturezas_juridicas` | `codigo` |
| `qualificacoes_socios` | `codigo` |
| `motivos` | `codigo` |
| `paises` | `codigo` |
| `situacoes_cadastrais` | `codigo` |
| `portes` | `codigo` |
| `matriz_filial` | `codigo` |
| `municipios_por_uf` | (`uf`, `municipio_codigo`) |
| `cnaes_presentes` | `cnae_codigo` |
| `estabelecimentos_listagem` | `cnpj_completo` |

### 3.3 Consumidores futuros (API e BI)

O schema é a base para uma futura API para terceiros e para ferramentas de BI, mas nenhum
desses consumidores faz parte deste MVP.

- A tabela `estabelecimentos_listagem` é o **read model estável** para listagens; a API e o
  BI não devem depender de tabelas de staging.
- `query_metadata` é a fonte de competência, fonte, data de carga e contagens.
- Uma futura API deve usar o mesmo papel PostgreSQL somente leitura, as mesmas regras de
  mascaramento e um contrato versionado próprio; autenticação, quotas e rate limiting são
  requisitos da feature de API, não deste contrato.
- O Superset pode conectar-se diretamente com papel somente leitura, mas deve usar banco de
  metadados próprio e, em produção, uma réplica de leitura/base analítica para não competir
  com a web.
- Nomes de tabelas/colunas e o significado de `query_metadata` são o contrato; mudanças
  exigem atualização deste documento e versionamento.

### 3.4 Índices mínimos

| Tabela | Índice | Finalidade |
|---|---|---|
| `estabelecimentos_listagem` | `(uf, municipio_codigo)` | UF + município |
| `estabelecimentos_listagem` | `(situacao_codigo)` | situação |
| `estabelecimentos_listagem` | `(porte_codigo)` | porte |
| `estabelecimentos_listagem` | `(data_inicio_atividade)` | intervalo de abertura |
| `estabelecimentos_listagem` | `(cnpj_basico)` | junções |
| `estabelecimentos_listagem` | GIN `razao_social_normalizada` | busca literal por substring |
| `estabelecimento_cnaes` | `(cnae_codigo)` | filtro CNAE |
| `socios` | `(cnpj_basico)` | detalhe |

Índices adicionais podem ser avaliados com `EXPLAIN ANALYZE`; o benchmark registra o
resultado e justifica qualquer inclusão.

### 3.5 Contagens e metadados

- `query_metadata.total_*` são preenchidos na publicação e usados para a contagem sem
  filtros.
- Consultas filtradas usam `COUNT(*)` sobre `estabelecimentos_listagem` (e
  `estabelecimento_cnaes` quando houver CNAE).
- A contagem exibida é a do conjunto filtrado, não a da página.

---

## 4. Regras de Compatibilidade

1. **Origem**: alteração de nome/tipo de coluna em `schema.py` exige atualização do loader,
   deste contrato e da versão do artefato.
2. **Competência**: é imutável; reexecutar substitui o retrato de forma atômica.
3. **Schema de consulta**: adicionar coluna/tabela é permitido com atualização deste
   contrato; remover/renomear exige nova versão.
4. **Privacidade**: nenhum campo de CPF em claro pode ser adicionado ao schema de consulta.
5. **Sem fontes externas**: nenhuma tabela do schema pode ser preenchida por fonte que não
   os Parquets da competência ativa.
6. **Sem busca fuzzy**: `pg_trgm` é usado apenas para indexar substring literal; não há
   índice/coluna de similaridade.

---

## 5. Gates de Publicação

Antes de tornar a competência visível:

- [ ] As 37 famílias de arquivos esperadas foram encontradas e o schema foi validado.
- [ ] CNPJs completos têm 14 dígitos e as chaves são únicas.
- [ ] Contagens de empresas, estabelecimentos e sócios conferem com os arquivos de origem.
- [ ] Todo `estabelecimento_cnaes.cnpj_completo` existe em `estabelecimentos`.
- [ ] Nenhum CPF em claro existe em `socios`.
- [ ] `estabelecimentos_listagem` tem uma linha por estabelecimento.
- [ ] Índices e `ANALYZE` foram executados.
- [ ] `query_metadata` foi atualizado na mesma transação da troca.
- [ ] A aplicação responde `health` com a competência ativa.
- [ ] O benchmark registrou p95 de listagem/detalhe e o custo da carga.

---

## 6. Referências

- [Modelo de dados](../data-model.md)
- [Contrato HTTP](../contracts/query-api.md)
- [Pesquisa](../research.md)
- [Especificação](../spec.md)
- `schema.py`, `conversor.py`, `src/conversao/bronze.py` — origem dos Parquets
