# Data Model: Prospecção Comercial B2B

**Feature**: 004-prospeccao-comercial-b2b  
**Fonte**: Parquets Bronze da competência `2026-10` (Receita Federal)  
**Granularidade**: uma linha por estabelecimento (CNPJ completo)

## Read model principal: `estabelecimentos_listagem`

Chave: `cnpj_completo char(14) PRIMARY KEY`

### Colunas existentes (reaproveitadas)

| Coluna | Origem | Uso na prospecção |
|---|---|---|
| `cnpj_completo` | Estabelecimentos | identidade da linha |
| `cnpj_basico` | Estabelecimentos | agrupar matriz/filiais |
| `razao_social` / `razao_social_normalizada` | Empresas | identificação e busca |
| `nome_fantasia` | Estabelecimentos | identificação comercial |
| `situacao_codigo` / `situacao_descricao` | Estabelecimentos | filtro (padrão Ativa) |
| `uf` / `municipio_codigo` / `municipio_descricao` | Estabelecimentos | recorte regional |
| `cnae_principal_codigo` / `cnae_principal_descricao` | Estabelecimentos | atividade e segmento |
| `cnae_secundaria_texto` | Estabelecimentos | CNAEs secundários (texto original) |
| `identificador_matriz_filial` / `matriz_filial_descricao` | Estabelecimentos | matriz/filial |
| `data_inicio_atividade` | Estabelecimentos | idade |
| `porte_codigo` / `porte_descricao` | Empresas | filtro de porte |
| `optante_simples` / `optante_mei` | Simples | filtro tributário |

### Novas colunas

| Coluna | Tipo | Regra |
|---|---|---|
| `cep` | `char(8)` | CEP do estabelecimento (somente dígitos) |
| `ddd_1`, `telefone_1` | `text` | telefone cadastral 1 |
| `correio_eletronico` | `text` | e-mail cadastral |
| `natureza_juridica_codigo` | `text` | código da natureza jurídica |
| `natureza_juridica_descricao` | `text` | descrição da natureza jurídica |
| `capital_social` | `numeric(18,2)` | capital social da empresa |
| `segmento_codigo` | `text` | FK lógica para `segmentos.codigo` |
| `segmento_nome` | `text` | nome do segmento (desnormalizado) |
| `qtd_cnaes` | `integer NOT NULL DEFAULT 0` | principal + secundários distintos |
| `qtd_estabelecimentos` | `integer NOT NULL DEFAULT 0` | estabelecimentos do CNPJ raiz |
| `qtd_filiais` | `integer NOT NULL DEFAULT 0` | estabelecimentos com indicador filial |

### Índices novos

```sql
CREATE INDEX ... ON estabelecimentos_listagem (segmento_codigo);
CREATE INDEX ... ON estabelecimentos_listagem (segmento_codigo, uf);
CREATE INDEX ... ON estabelecimentos_listagem (qtd_filiais);
```

O índice `(segmento_codigo, uf)` atende às duas agregações da página sem full scan
desnecessário quando o recorte é só segmento/UF.

## Catálogo de segmentos

Tabelas de apoio publicadas pela carga:

```sql
CREATE TABLE segmentos (
    codigo text PRIMARY KEY,
    nome text NOT NULL
);

CREATE TABLE segmento_divisoes (
    divisao char(2) PRIMARY KEY,
    segmento_codigo text NOT NULL REFERENCES segmentos(codigo)
);
```

Mapa por divisão CNAE (2 primeiros dígitos):

| Divisões | Código | Segmento |
|---|---|---|
| 01–03, 05–09 | `AGRO` | Agronegócio e extrativismo |
| 10–33 | `INDUSTRIA` | Indústria e transformação |
| 35–39 | `UTILIDADES` | Energia, água e saneamento |
| 41–43 | `CONSTRUCAO` | Construção |
| 45 | `VEICULOS` | Veículos e peças |
| 46 | `ATACADO` | Distribuição e atacado |
| 47 | `VAREJO` | Comércio varejista |
| 49–53 | `TRANSPORTE` | Transporte e logística |
| 55–56 | `ALOJAMENTO` | Alojamento e alimentação |
| 58–63 | `TECNOLOGIA` | Informação e comunicação |
| 64–66 | `FINANCEIRO` | Financeiro e seguros |
| 68 | `IMOBILIARIO` | Imobiliário |
| 69–75 | `SERVICOS_PROFISSIONAIS` | Serviços profissionais e técnicos |
| 77–82 | `SERVICOS_APOIO` | Serviços administrativos e apoio |
| 84 | `ADMIN_PUBLICA` | Administração pública |
| 85 | `EDUCACAO` | Educação |
| 86–88 | `SAUDE` | Saúde e assistência social |
| 90–93 | `CULTURA` | Artes, cultura e recreação |
| 94–96 | `OUTROS_SERVICOS` | Outros serviços |
| 97–99 ou ausente | `NAO_CLASSIFICADO` | Não classificado |

## Regras dos indicadores derivados

- **Idade da empresa**: anos completos entre `data_inicio_atividade` e o último dia da
  competência; `NULL`/"não informada" quando a data não existe.
- **Faixa de idade em filtros**:
  - `idade_min = n` → `data_inicio_atividade <= referência - n anos`;
  - `idade_max = n` → `data_inicio_atividade > referência - (n + 1) anos`.
  - Referência = último dia da competência (ex.: `2026-10-31`).
- **`qtd_cnaes`**: `count(*)` em `estabelecimento_cnaes` por `cnpj_completo` (o loader já
  deduplica o principal e os secundários).
- **`qtd_estabelecimentos`**: `count(*)` por `cnpj_basico`.
- **`qtd_filiais`**: `count(*) FILTER (WHERE identificador_matriz_filial = '2')` por
  `cnpj_basico`.
- **Presença de contato**: `ddd_1 + telefone_1` preenchidos **ou** `correio_eletronico`
  preenchido.

## Filtros (`EstabelecimentoFiltros`)

| Parâmetro | Validação | Observação |
|---|---|---|
| `segmento` | código existente no catálogo | opcional |
| `uf` | UF válida | existente |
| `municipio` | 4–7 dígitos, pertencente à UF | existente |
| `cnae` | 7 dígitos | existente |
| `porte` | código de porte | existente |
| `situacao` | código de situação | página assume `02` por padrão |
| `matriz_filial` | `1`/`2` | existente |
| `simples` / `mei` | `sim`/`nao`/`qualquer` | existente |
| `idade_min` / `idade_max` | inteiro 0–200, mínimo ≤ máximo | novo |
| `filiais_min` / `filiais_max` | inteiro 0–10000, mínimo ≤ máximo | novo |
| `com_contato` | `sim`/`nao`/`qualquer` | novo |
| `pagina` | inteiro ≥ 1 | existente |

## Agregações da página

- **Por segmento**: `segmento_nome`, `count(*)`, ordenado desc, limitado aos 20
  segmentos do catálogo.
- **Por UF**: `uf`, `count(*)`, ordenado desc.
- Ambas usam exatamente o mesmo `WHERE` da listagem e o mesmo recorte de competência.

Para volume real, o cubo `estabelecimentos_filtros_stats` inclui `segmento_codigo` e as
dimensões de baixa cardinalidade. Quando o recorte só usa essas dimensões, contagem e
agregações leem o cubo; caso contrário, usam a listagem com `COUNT_CAP`. Se o cubo
estiver vazio (schema migrado sem recarga), a contagem cai automaticamente para o
caminho seguro, em vez de exibir zero.

## Exportação CSV

Cabeçalho (português, separador `;`):

```
cnpj;cnpj_raiz;razao_social;nome_fantasia;segmento;situacao;uf;municipio;cep;
cnae_principal;qtd_cnaes;porte;natureza_juridica;capital_social;data_abertura;
idade_anos;matriz_filial;qtd_estabelecimentos;qtd_filiais;telefone;email;simples;mei;
cnaes_secundarios
```

- Teto: `CNPJ_QUERY_EXPORT_LIMIT` (padrão 50.000, faixa 1–100.000).
- Sem CPF, sem sócios.
- Valores iniciados por `=`, `+`, `-`, `@` recebem prefixo `'`.
