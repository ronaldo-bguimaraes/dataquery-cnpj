# Checklist de requisitos: DAG manual Raw → Bronze por arquivo

**Feature**: [spec.md](../spec.md)
**Atualizado**: 2026-10-27

## Qualidade da especificação

- [x] Objetivo e valor para o usuário estão claros.
- [x] Há uma única DAG manual, disparada uma vez por competência, sem schedule.
- [x] O fluxo descreve duas tarefas encadeadas por ZIP: `baixar_raw` → `converter_bronze`.
- [x] Cada ZIP selecionado tem download Raw seguido de conversão Bronze.
- [x] Raw existente é reutilizado, conta no limite e permanece selecionado para conversão.
- [x] Limite e ordenação determinística estão definidos antes da checagem de Raw.
- [x] Localizações de saída estão definidas: `raw/<AAAA-MM>/<nome-original>` e
      `bronze/<AAAA-MM>/<nome-base>.parquet`.
- [x] O modo local usa o filesystem em `data/` na raiz, montado como `/data` nos containers
      que executam tarefas.
- [x] Download e conversão acessam o armazenamento por uma interface genérica, sem depender
      diretamente de filesystem, bucket ou provedor.
- [x] Em produção, o backend pode ser selecionado como Cloudflare R2 via API compatível com
      S3 por configuração externa.
- [x] Falhas e retries são isolados por arquivo e visíveis no estado final da DAG.
- [x] Falha de download impede somente a conversão daquele arquivo; demais continuam.
- [x] Falha de conversão preserva o Raw e não publica Parquet parcial.
- [x] Retenção Raw por competência está alinhada à Constituição 1.1.0.
- [x] Prefixos, configuração e dados que podem trafegar por XCom estão definidos.
- [x] Fora de escopo: camadas Silver/Gold, schedule, frontend e API. O provisionamento
      operacional do R2 fica fora do escopo; a seleção do backend por configuração faz parte.
- [x] Requisitos são testáveis e não dependem de detalhes internos de implementação.

## Critérios de aceite verificáveis

- [x] Quickstart tem execução local mínima com `limite=1` e checagem Raw/Bronze; a troca do
      backend MinIO pelo filesystem será feita na fase de plano.
- [x] Plano especifica mapping dinâmico e dependência por arquivo; o fluxo não muda com esta
      troca de armazenamento.
- [x] Contratos descrevem as fronteiras de download, conversão e storage; a interface genérica
      será detalhada no plano.
- [x] Tasks discriminam a fundação já entregue do novo trabalho pendente.
- [x] Implementação e validação E2E ficam fora do escopo desta atualização; serão
      replanejadas para os backends local e configurável.

## Observações

Esta revisão atualizou apenas `spec.md` e este checklist para remover a dependência de MinIO
e definir a interface genérica de armazenamento. Plano, tarefas, contratos, quickstart e
código existentes ainda refletem o desenho anterior e não foram alterados nesta etapa. Nenhum
teste ou download foi executado nesta atualização documental.
