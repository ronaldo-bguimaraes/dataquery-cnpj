# Quickstart: Prospecção Comercial B2B

## Pré-requisitos

- Docker + Compose v2;
- `.env` na raiz (a partir de `.env.example`);
- Parquets Bronze da competência `2026-10` em `data/bronze/2026-10/`.

## Subir e carregar

```shell
docker compose --profile query up -d query-db
docker compose --profile query run --rm query-loader
docker compose --profile query up -d query-web
```

A carga local aplica o schema novo (colunas e tabelas de prospecção) de forma
idempotente e reconstrói o read model.

## Usar

1. Abra <http://localhost:8000/prospeccao>.
2. A situação já vem em **Ativa**. Escolha um segmento (por exemplo,
   "Distribuição e atacado") e uma UF.
3. Confira a contagem, as tabelas "Estabelecimentos por segmento" e
   "Estabelecimentos por UF" e a listagem paginada.
4. Clique em **Gerar lista CSV** para baixar o recorte.

## Exportação

```shell
curl -sS "http://localhost:8000/prospeccao.csv?segmento=ATACADO&uf=SP" \
  -o prospeccao-atacado-sp.csv
head -n 2 prospeccao-atacado-sp.csv
```

- Teto padrão: 50.000 linhas (`CNPJ_QUERY_EXPORT_LIMIT`).
- Sem CPF e sem dados de sócios.
- Campos com `;`/aspas são escapados; fórmulas são neutralizadas.

## Testes

```shell
docker compose --profile query-test run --rm query-tests \
  pytest tests/query -q -m "not e2e"
```

Ou, com PostgreSQL de teste acessível em `127.0.0.1:55432`:

```shell
python -m pytest tests/query -q -m "not e2e"
```

## Verificação rápida do read model

```sql
SELECT segmento_nome, count(*) AS total
FROM estabelecimentos_listagem
GROUP BY 1
ORDER BY total DESC;

SELECT cnpj_basico, qtd_estabelecimentos, qtd_filiais
FROM estabelecimentos_listagem
WHERE qtd_filiais > 0
LIMIT 5;
```

## Recarga e mudanças de catálogo

Alterar `src/query/domain/segmentos.py` exige rodar novamente o loader para republicar
`segmentos`, `segmento_divisoes` e `segmento_codigo` no read model. A carga é idempotente
e substitui o retrato.
