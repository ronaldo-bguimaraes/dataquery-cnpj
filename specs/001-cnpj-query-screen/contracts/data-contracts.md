# Contrato dos Artefatos de Dados (Parquet)

**Feature**: `001-cnpj-query-screen` · **Data**: 2026-10-22
**Escopo**: formato, layout e regras de compatibilidade dos Parquet gravados no bucket — tanto
os canônicos (existentes) quanto os derivados (novos).
**Fonte de verdade dos nomes de coluna**: `schema.py` no repositório (Princípio II da
constituição). Este documento descreve o contrato **físico**; o **lógico** está no código.

---

## 1. Nomenclatura de Caminhos

```text
s3://<bucket>/cnpj/competencia=<AAAA-MM>/tabela=<nome>/uf=<SIGLA>/part-<NNNNN>.parquet
```

| Segmento | Formato | Exemplo |
|---|---|---|
| `competencia` | `AAAA-MM` | `competencia=2026-02` |
| `tabela` | nome do conjunto em `schema.py` | `tabela=empresas` |
| `uf` | sigla de 2 letras (particionamento Hive) | `uf=SP` |
| `part` | 5 dígitos, sequencial por partição | `part-00000.parquet` |

Tabelas de apoio (lookups) não são particionadas por UF:
`s3://<bucket>/cnpj/competencia=<AAAA-MM>/tabela=<nome>/part-<NNNNN>.parquet`

## 2. Propriedades Físicas

| Propriedade | Valor | Motivo |
|---|---|---|
| `write_statistics` | `True` | sem statistics não há zonemap; toda consulta vira varredura completa |
| Row group | 100 mil – 1 milhão de linhas | row groups < 5 mil linhas são 5–10× mais lentos em leitura remota |
| Compressão | ZSTD | melhor razão bytes/latência |
| Formato | Parquet | portável entre DuckDB, Spark, Trino e Iceberg |
| Encoding | dicionário para colunas de texto repetitivo | reduz o tamanho e acelera a decodificação |
| Ordenação | por chave de consulta (ver §3) | poda por min/max dentro do row group |

## 3. Ordenação por Tabela

| Tabela | Particionamento | Ordenação dentro da partição |
|---|---|---|
| `empresas` | `uf` derivada do CNPJ | `cnpj_basico` |
| `estabelecimentos` | `estabelecimento_uf` | `estabelecimento_cnpj_basico`, `estabelecimento_cnpj_ordem` |
| `socios` | `uf` derivada do CNPJ | `socio_cnpj_basico`, `socio_identificador_socio` |
| `simples` | `uf` derivada do CNPJ | `simples_cnpj_basico` |
| `busca_documentos` | `uf` derivada do CNPJ | `cnpj_basico` |
| `busca_postings` | `uf` derivada do CNPJ | `trigrama`, `cnpj_basico` |
| lookups | — | `codigo` |

## 4. Tabelas Canônicas

Os nomes de coluna são os de `schema.py` e **não mudam** nesta feature:

- `empresas`: `empresa_cnpj_basico`, `empresa_razao_social`, `empresa_natureza_juridica`,
  `empresa_qualificacao_responsavel`, `empresa_capital_social`, `empresa_porte`,
  `empresa_ente_federativo`
- `estabelecimentos`: 30 colunas (`estabelecimento_*`)
- `socios`: 11 colunas (`socio_*`)
- `simples`: 7 colunas (`simples_*`)
- lookups: `paises`, `municipios`, `qualificacoes_socios`, `naturezas_juridicas`, `cnaes`,
  `motivos`

**Observação**: `empresas` **não tem** coluna de UF. A UF usada no particionamento é derivada do
CNPJ e **não** é gravada como coluna na tabela canônica — quem a materializa é
`busca_documentos`.

## 5. Tabelas Derivadas

### 5.1 `busca_documentos`

Uma linha por empresa.

| Coluna | Tipo | Descrição |
|---|---|---|
| `cnpj_basico` | texto | 8 dígitos; chave de junção com `empresas` |
| `cnpj` | texto | 14 dígitos; usado como chave de retorno |
| `razao_social` | texto | razão social original |
| `razao_social_norm` | texto | minúsculas, sem acento, sem pontuação, espaços colapsados |
| `uf` | texto | sigla de 2 letras, derivada do CNPJ |
| `municipio` | texto | código do município do **estabelecimento matriz**; nulo se ausente |
| `porte` | texto | código de porte |
| `cnae_principal` | texto | CNAE principal do estabelecimento matriz |
| `trigramas` | lista de texto | trigramas de `razao_social_norm` sem espaços, em ordem |

### 5.2 `busca_postings`

Uma linha por (empresa, trigrama selecionado).

| Coluna | Tipo | Descrição |
|---|---|---|
| `cnpj_basico` | texto | 8 dígitos |
| `trigrama` | texto | 3 caracteres |
| `df` | inteiro | frequência do trigrama no corpus (mesma em todas as linhas do trigrama) |

**Seleção dos trigramas**: de cada documento são mantidos no máximo **8 trigramas** — os de
menor frequência dentro do próprio nome, com desempate por ordem alfabética. A lista completa
permanece em `busca_documentos.trigramas`, usada no ranqueamento final.

## 6. Regras de Compatibilidade

1. **Adição de coluna** em tabela canônica ou derivada: permitida, exige atualizar `schema.py`,
   o README e a versão do contrato.
2. **Remoção ou renomeação de coluna**: exige nova versão do contrato e um período de
   convivência.
3. **Mudança de tipo**: proibida dentro da mesma competência.
4. **Mudança de layout** (particionamento, ordenação, compressão): invalida o cache local do
   DuckDB e exige reindexação — precisa ser incrementada na versão do artefato.
5. **Competência é imutável**: reprocessar a mesma competência substitui os artefatos de forma
   atômica (escrita em prefixo temporário + renomeação), nunca parcial.

## 7. Regras de Publicação

| Etapa | Verificação obrigatória |
|---|---|
| Após gravar | contagem de linhas > 0 e `write_statistics` ativo (verificar pelo *footer* do Parquet) |
| Após gravar | soma das linhas por partição = total esperado da competência |
| Após derivar | `busca_documentos` tem exatamente 1 linha por empresa de `empresas` |
| Após derivar | todo `cnpj_basico` de `busca_postings` existe em `busca_documentos` |
| Antes de publicar | nenhum valor de `socio_cnpj_cpf_socio` fora da tabela `socios` |

## 8. Referências

- Colunas canônicas: `schema.py`
- Conversão: `conversor.py`, `read_files.py`
- Decisões de layout: [research.md](../research.md) D2 e D4
- Entidades e validações: [data-model.md](../data-model.md)
