# Contrato da API de Consulta (v1)

**Feature**: `001-cnpj-query-screen` · **Data**: 2026-10-22
**Implementação**: serviço Python (FastAPI) + DuckDB, exposto na rede interna.
**Consumidores**: route handler do Next.js (`web/app/api/consulta/route.ts`) — **o browser
nunca chama esta API diretamente**.

---

## 1. Convenções

| Item | Regra |
|---|---|
| Versão | prefixo de caminho `/v1` |
| Formato | `application/json; charset=utf-8` |
| Datas | `AAAA-MM-DD` (datas) e `AAAA-MM` (competência) |
| Decimais | ponto como separador; `capital_social` com 2 casas |
| Valores nulos | `null` (a chave permanece presente) |
| Autenticação | **nenhuma** (FR-018) — o serviço só é alcançável pela rede interna |
| Cache | respostas com dado de empresa usam `Cache-Control: private, no-store` (dado pessoal<br>mascarado); listas filtradas podem usar `private, max-age=60` |
| Idempotência | todas as rotas são `GET` e somente leitura |

**Envelope de sucesso**

```json
{
  "competencia": "2026-02",
  "dados": { }
}
```

**Envelope de erro**

```json
{
  "erro": {
    "codigo": "CNPJ_INVALIDO",
    "mensagem": "O CNPJ informado não é válido.",
    "detalhes": { "campo": "cnpj" }
  }
}
```

| Código | HTTP | Quando |
|---|---|---|
| `CNPJ_INVALIDO` | 400 | CNPJ com formato ou dígitos verificadores inválidos |
| `PARAMETRO_INVALIDO` | 400 | filtro, ordenação ou paginação fora do domínio aceito |
| `COMPETENCIA_INDISPONIVEL` | 404 | competência pedida não existe no bucket |
| `EMPRESA_NAO_ENCONTRADA` | 404 | CNPJ válido sem registro na competência |
| `RESULTADO_VAZIO` | 200 | busca sem correspondência (não é erro) |
| `LIMITE_EXCEDIDO` | 413 | paginação além do máximo permitido |
| `ERRO_INTERNO` | 500 | falha inesperada (nenhum detalhe interno exposto) |

---

## 2. `GET /v1/empresas/{cnpj}` — ficha da empresa

Retorna a ficha completa: cadastro, estabelecimentos, sócios e regime tributário.

**Parâmetros de caminho**

| Nome | Tipo | Obrigatório | Regras |
|---|---|---|---|
| `cnpj` | texto | sim | 14 dígitos, aceitos com máscara, barras ou espaços; normalizado para só dígitos |

**Parâmetros de query**

| Nome | Tipo | Padrão | Regras |
|---|---|---|---|
| `competencia` | `AAAA-MM` | competência mais recente disponível | deve existir no bucket |

**Resposta 200** — `dados`:

```json
{
  "competencia": "2026-02",
  "dados": {
    "empresa": {
      "cnpj": "00038174000143",
      "cnpj_basico": "00038174",
      "razao_social": "BRASILIA DISTRIBUIDORA DE ALIMENTOS LTDA",
      "natureza_juridica": { "codigo": "2062", "descricao": "Sociedade Empresária Limitada" },
      "qualificacao_responsavel": { "codigo": "10", "descricao": "Diretor" },
      "capital_social": 150000.00,
      "porte": "05",
      "ente_federativo": null,
      "uf": "DF",
      "cnae_principal": { "codigo": "4631701", "descricao": "Comércio atacadista de produtos alimentícios" }
    },
    "estabelecimentos": [
      {
        "cnpj": "00038174000143",
        "identificador_matriz_filial": "1",
        "nome_fantasia": "BRASILIA ALIMENTOS",
        "situacao_cadastral": { "codigo": "02", "descricao": "Ativa" },
        "data_situacao_cadastral": "2005-11-03",
        "data_inicio_atividade": "2005-11-03",
        "cnae_principal": { "codigo": "4631701", "descricao": "Comércio atacadista de produtos alimentícios" },
        "cnaes_secundarios": ["4711104"],
        "endereco": {
          "logradouro": "SETOR DE INDUSTRIA", "numero": "S/N", "complemento": "GALPAO 12",
          "bairro": "SAIA", "cep": "71800000", "uf": "DF",
          "municipio": { "codigo": "9701", "descricao": "BRASILIA" }
        }
      }
    ],
    "socios": [
      {
        "identificador_socio": "2",
        "nome_socio": "MARIA EXEMPLO",
        "cpf_socio": "***456789**",
        "qualificacao_socio": { "codigo": "22", "descricao": "Sócio-Administrador" },
        "data_entrada_sociedade": "2005-11-03",
        "pais": { "codigo": "1058", "descricao": "BRASIL" },
        "representante_legal": "0",
        "nome_representante": null,
        "qualificacao_representante_legal": null,
        "faixa_etaria": "05"
      }
    ],
    "regime_tributario": {
      "opcao_simples": "2006-01-01",
      "data_opcao_simples": "2006-01-01",
      "data_exclusao_simples": null,
      "opcao_mei": null,
      "data_opcao_mei": null,
      "data_exclusao_mei": null
    }
  }
}
```

**Regras da resposta**

- `socios[].cpf_socio` é **sempre** mascarado (`***456789**`) — a máscara é aplicada na fronteira
  do serviço; o valor em claro não existe na resposta nem em log (FR-017, FR-018).
- Seções sem registro vêm como lista vazia ou `null` — nunca como erro (FR-015).
- `empresa.uf` é a UF **derivada do CNPJ**; `estabelecimentos[].endereco.uf` é a UF do endereço.
- `404` com `EMPRESA_NAO_ENCONTRADA` quando o CNPJ é válido mas não consta na competência.

---

## 3. `GET /v1/empresas` — listagem por filtros

**Parâmetros de query**

| Nome | Tipo | Obrigatório | Regras |
|---|---|---|---|
| `uf` | texto | não | sigla válida (27) |
| `municipio` | texto | não | código de município existente |
| `cnae` | texto | não | código de 7 dígitos **ou prefixo** (hierárquico, FR-009); casa CNAE principal e secundário |
| `porte` | texto | não | valor do conjunto fechado; repetir o parâmetro para OR |
| `razao_social` | texto | não | **correspondência exata/prefixo normalizado** (sem fuzzy) — para fuzzy use a rota 4 |
| `ordenar_por` | enum | não | `razao_social` \| `municipio` \| `porte` \| `cnpj` (padrão `cnpj`) |
| `direcao` | enum | não | `asc` \| `desc` (padrão `asc`) |
| `pagina` | inteiro | não | ≥ 1 (padrão 1) |
| `tamanho_pagina` | inteiro | não | 1–100 (padrão 20) |
| `competencia` | `AAAA-MM` | não | padrão: mais recente disponível |

**Resposta 200**

```json
{
  "competencia": "2026-02",
  "dados": {
    "itens": [
      {
        "cnpj": "00038174000143",
        "razao_social": "BRASILIA DISTRIBUIDORA DE ALIMENTOS LTDA",
        "uf": "DF",
        "municipio": { "codigo": "9701", "descricao": "BRASILIA" },
        "porte": "05",
        "cnae_principal": { "codigo": "4631701", "descricao": "Comércio atacadista de produtos alimentícios" }
      }
    ],
    "paginacao": { "pagina": 1, "tamanho_pagina": 20, "total": 137, "total_paginas": 7 },
    "filtros_aplicados": { "uf": "DF", "cnae": "4631701" }
  }
}
```

**Regras**

- Todos os filtros são combinados com **AND**; valores repetidos do mesmo filtro usam OR.
- `filtros_aplicados` devolve os filtros normalizados efetivamente usados — a listagem precisa
  preservar os filtros entre as páginas (FR-010).
- `total` é o total de correspondências, não o da página.

---

## 4. `GET /v1/busca/razao-social` — busca tolerante a erro de digitação

**Parâmetros de query**

| Nome | Tipo | Obrigatório | Regras |
|---|---|---|---|
| `termo` | texto | sim | ≥ 3 caracteres (abaixo disso o corte de trigramas não se aplica) |
| `uf` | texto | não | restringe os candidatos antes do ranqueamento |
| `municipio` | texto | não | idem |
| `cnae` | texto | não | idem |
| `porte` | texto | não | idem |
| `limite` | inteiro | não | 1–100 (padrão 20) |
| `limiar_similaridade` | decimal | não | 0–1 (padrão 0,25) |
| `competencia` | `AAAA-MM` | não | padrão: mais recente disponível |

**Resposta 200**

```json
{
  "competencia": "2026-02",
  "dados": {
    "itens": [
      {
        "cnpj": "00038174000143",
        "razao_social": "BRASILIA DISTRIBUIDORA DE ALIMENTOS LTDA",
        "trecho_destacado": "BRASILIA",
        "similaridade": 0.62,
        "uf": "DF",
        "municipio": { "codigo": "9701", "descricao": "BRASILIA" },
        "porte": "05",
        "cnae_principal": { "codigo": "4631701", "descricao": "Comércio atacadista de produtos alimentícios" }
      }
    ],
    "total_candidatos": 412,
    "termo": "BRASLIA"
  }
}
```

**Regras**

- `similaridade` é a similaridade de dice sobre trigramas, na faixa 0–1.
- `trecho_destacado` é o trecho da razão social que originou a correspondência (FR-013).
- A ordenação é por `similaridade` decrescente, com desempate por porte e depois por CNPJ.
- `total_candidatos` é o total antes do corte de `limite` — o usuário precisa saber que houve
  mais candidatos do que os exibidos.

---

## 5. `GET /v1/competencias` — competências disponíveis

```json
{
  "dados": {
    "competencias": ["2026-02", "2026-01", "2025-12"],
    "atual": "2026-02"
  }
}
```

---

## 6. `GET /v1/lookups/{recurso}` — valores dos filtros

Recursos: `uf` · `municipio` (com `uf` como query opcional) · `cnae` · `porte`.

```json
{
  "competencia": "2026-02",
  "dados": {
    "itens": [ { "codigo": "SP", "descricao": "São Paulo" } ]
  }
}
```

---

## 7. `GET /v1/saude` — prontidão

```json
{ "dados": { "status": "ok", "competencia": "2026-02" } }
```

Retorna 503 quando a competência configurada não está acessível — usado pelo Worker para
exibir "dados indisponíveis" em vez de erro genérico.

---

## 8. Regras transversais

1. **Erros de origem nunca vazam**: o Worker e a API exibem `mensagem` ao usuário, e o detalhe
   técnico fica apenas no log da aplicação (sem dado pessoal).
2. **Idempotência e leitura pura**: nenhuma rota altera estado; não há `POST`/`PUT`/`DELETE` na
   v1.
3. **Desempenho alvo**: `GET /v1/empresas/{cnpj}` ≤ 3 s (p95) e `GET /v1/empresas` ≤ 5 s (p95)
   (SC-002, SC-003).
4. **Compatibilidade**: enquanto a v1 estiver publicada, novos campos podem ser adicionados às
   respostas; campos existentes não mudam de tipo nem de significado. Remoções exigem v2.
