# Contrato HTTP — Páginas do Explorador

**Feature**: `001-cnpj-query-screen` · **Data**: 2026-10-27
**Tipo**: aplicação web server-rendered (FastAPI + Jinja2)
**Consumidor**: navegador da pessoa interessada
**API JSON pública**: não existe no MVP

---

## 1. Convenções

| Item | Regra |
|---|---|
| Métodos | somente `GET`; nenhuma rota escreve dados |
| Formato | HTML UTF-8; `GET /health` responde JSON |
| Filtros | na query string, um valor por filtro |
| Estado | vive na URL; não há sessão, cookie de preferência ou login |
| Competência | `2026-10`, exibida em todas as páginas de dados |
| Fonte | `Receita Federal`, exibida junto da competência |
| Aviso | toda página de dados informa que é um retrato mensal, não tempo real |
| Cache | páginas de dados com `Cache-Control: no-store`; `health` também |
| Exportação | nenhuma rota, botão ou link de download/CSV |
| Privacidade | CPF nunca completo; termos de busca não entram em log |
| Erros | HTML compreensível, sem stack trace, SQL ou detalhe interno |

### 1.1 Envelope visual comum

Todas as páginas de dados exibem:

- título da página;
- competência `2026-10`;
- fonte `Receita Federal`;
- aviso de retrato mensal;
- link para a listagem (quando aplicável);
- nenhum controle de exportação/download.

---

## 2. `GET /` — entrada

Redireciona (`303`) para `/estabelecimentos`. Não executa consulta.

---

## 3. `GET /estabelecimentos` — listagem e filtros

Retorna a página de exploração com filtros, contagem total, listagem paginada e estados.

### 3.1 Parâmetros

| Nome | Tipo | Obrigatório | Regras | Efeito |
|---|---|---|---|---|
| `q` | texto | não | CNPJ completo (14 dígitos, com/sem máscara) ou nome com ≥ 3 caracteres | busca literal; não há fuzzy |
| `situacao` | texto | não | código do catálogo (`01`,`02`,`03`,`04`,`05`,`08`) | situação cadastral |
| `uf` | texto | não | 2 letras existentes no inventário | UF do estabelecimento |
| `municipio` | texto | não | código presente em `municipios_por_uf` para a UF | município do estabelecimento |
| `cnae` | texto | não | 7 dígitos presente em `cnaes_presentes` | CNAE principal ou secundário |
| `porte` | texto | não | `00`,`01`,`03`,`05` | porte da empresa |
| `matriz_filial` | texto | não | `1` ou `2` | matriz ou filial |
| `abertura_de` | data | não | `AAAA-MM-DD` | início do intervalo de abertura (inclusivo) |
| `abertura_ate` | data | não | `AAAA-MM-DD`, ≥ `abertura_de` | fim do intervalo de abertura (inclusivo) |
| `simples` | texto | não | `sim`, `nao` ou `qualquer` | opção pelo Simples Nacional |
| `mei` | texto | não | `sim`, `nao` ou `qualquer` | opção pelo MEI |
| `pagina` | inteiro | não | ≥ 1; padrão `1` | página da listagem |

- A página tem **25 registros** no MVP; não há parâmetro de tamanho de página.
- Filtros diferentes são cumulativos (`E`). Não há seleção de múltiplos valores no mesmo
  filtro.
- `q` que parece CNPJ é validado como CNPJ; se inválido, a página retorna `400`.
- Alterar qualquer filtro reinicia a navegação na página `1`.
- A listagem é ordenada por CNPJ completo, de forma estável.

### 3.2 Resposta 200 — com resultados

A página MUST conter:

- competência e fonte;
- total exato de estabelecimentos correspondentes;
- página atual e indicação de que há próxima/anterior;
- uma linha por estabelecimento com:
  - CNPJ completo;
  - razão social da empresa;
  - nome fantasia, quando houver;
  - situação cadastral (rótulo e/ou código);
  - UF e município;
  - CNAE principal;
  - matriz ou filial;
  - data de abertura;
  - indicador de opção pelo Simples/MEI;
- link para `/estabelecimentos/{cnpj}`;
- formulário de filtros preenchido com os valores atuais;
- nenhum botão de exportação/download.

### 3.3 Resposta 200 — sem resultados

A página MUST conter:

- contagem `0`;
- mensagem clara de que não há correspondências;
- ação para limpar os filtros;
- filtros atuais preservados no formulário;
- competência e fonte.

O caso é de resultado vazio, **não** de erro HTTP.

### 3.4 Respostas de erro

| HTTP | Quando | Conteúdo |
|---|---|---|
| `400` | parâmetro inválido: CNPJ, UF, município, CNAE, situação, porte, matriz/filial, data, página ou busca curta | página de erro com campo e orientação; sem consulta |
| `404` | rota inexistente | página 404 |
| `503` | competência indisponível, banco inacessível ou carga não publicada | página de indisponibilidade, sem resultados parciais |
| `500` | falha inesperada | mensagem genérica e opção de tentar novamente; sem detalhe interno |

---

## 4. `GET /estabelecimentos/{cnpj}` — detalhe

Retorna os detalhes cadastrais de um estabelecimento e os dados relacionados.

### 4.1 Caminho

| Nome | Regra |
|---|---|
| `cnpj` | CNPJ completo com 14 dígitos; aceita máscara (normalizada) |

### 4.2 Resposta 200

A página MUST conter:

- competência e fonte;
- CNPJ completo, matriz/filial, situação e data da situação;
- nome fantasia, quando houver;
- endereço: tipo de logradouro, logradouro, número, complemento, bairro, CEP, UF e
  município;
- data de abertura;
- CNAE principal e CNAEs secundários;
- contatos cadastrais quando disponíveis, identificados como dados da Receita Federal e
  sujeitos a ausência/desatualização;
- dados da empresa: razão social, natureza jurídica, porte, capital social, qualificação
  do responsável, CNAE principal e ente federativo, quando aplicável;
- situação de Simples Nacional e MEI, com datas de opção/exclusão ou indicação de ausência;
- sócios quando disponíveis, com nome/razão social, qualificação, data de entrada, país e
  documento sempre mascarado;
- link de retorno para a listagem que reconstrói a URL com os filtros e a página de origem.

### 4.3 Respostas de erro

| HTTP | Quando |
|---|---|
| `400` | CNPJ com formato/dígitos verificadores inválidos |
| `404` | CNPJ válido sem estabelecimento na competência |
| `503` | competência/banco indisponível |
| `500` | falha inesperada, sem detalhe interno |

---

## 5. `GET /health` — saúde operacional

Responde JSON mínimo, sem dados de empresas, CPF, credenciais ou detalhes internos:

```json
{
  "status": "ok",
  "competencia": "2026-10",
  "fonte": "Receita Federal",
  "banco": "ok"
}
```

- `200` quando o banco responde e há competência ativa.
- `503` quando o banco ou a competência não estão disponíveis.
- Não é uma API de consulta e não retorna registros.

---

## 6. Regras de Privacidade e Segurança

- CPF de sócio é sempre mascarado; nunca aparece com 11 dígitos.
- A apresentação também aplica redação defensiva a padrões de CPF em texto livre.
- Termos de busca, CPF e conteúdo de fichas não entram em log de aplicação.
- Toda saída HTML é escapada; entradas não são interpoladas em SQL.
- A aplicação usa um papel PostgreSQL somente leitura.
- Não há autenticação no MVP; a exposição deve permanecer local/interna até uma decisão
  de deployment com controle de acesso.
- Não há endpoints de exportação, download, edição ou upload.

---

## 7. Compatibilidade e Evolução

- A URL é o contrato de estado: filtros e página podem ser compartilhados e reabertos.
- Novos filtros podem ser adicionados como parâmetros opcionais, sem quebrar os existentes.
- Remoção/renomeação de parâmetro ou mudança de significado exige nova versão do contrato.
- Uma futura API JSON, exportação CSV, histórico ou autenticação serão features separadas;
  não fazem parte deste contrato.
