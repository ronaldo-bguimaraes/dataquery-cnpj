# Contrato de armazenamento — `ObjectStore`

**Feature**: `002-cnpj-download-dag`
**Consumidores**: listagem (preflight), `download_raw_file`, `convert_raw_file`, testes.

## Protocolo

Superfície mínima. Nomes abaixo são o contrato; a implementação pode usar dataclasses.

```python
class ObjectMetadata:
    key: str
    size: int

class ObjectStore(Protocol):
    def ping(self) -> None:
        """Preflight: raiz gravável ou bucket acessível. Sem credencial na exceção."""

    def head(self, key: str) -> ObjectMetadata | None:
        """None se ausente. size == 0 é presente mas inválido como Raw reutilizável."""

    def get_to_path(self, key: str, dest: Path) -> ObjectMetadata:
        """Copia o objeto para dest (arquivo local de scratch)."""

    def put_atomic(self, key: str, source: Path) -> ObjectMetadata:
        """Publica source na chave final só após a escrita completa."""

    def delete(self, key: str) -> None:
        """Idempotente; usado em testes. Produção não apaga Raw por política de retenção."""
```

Chaves são sempre lógicas (`raw/...`, `bronze/...`). O store rejeita chaves com `..`,
absolutas ou URI (`s3://`, `file://`).

`__repr__` e mensagens de erro **não** incluem secret key, access key, token WebDAV ou
query string.

## Fábrica

```python
def build_store_from_env() -> ObjectStore: ...
```

| `CNPJ_STORAGE_BACKEND` | Implementação | Obrigatório |
|---|---|---|
| `filesystem` (default se omitido no local) | `FilesystemObjectStore(root=CNPJ_DATA_ROOT)` | `CNPJ_DATA_ROOT` (`/data` nos containers) |
| `s3` | store S3-compatível (R2 ou outro) | endpoint, bucket, access key, secret key |

Valores inválidos de backend falham na fábrica, antes de I/O de dados.

## FilesystemObjectStore

- Resolve `key` para `Path(root) / key` (normalizado, contido em `root`).
- `ping`: `root` existe, é diretório, é gravável.
- `put_atomic`: escreve em `root/tmp/<uuid>` (ou sibling `.tmp`) e `os.replace` para o
  destino; cria diretórios pais; em falha remove o temporário e não substitui o destino.
- `head` de arquivo vazio devolve `size=0` (não reutilizável como Raw).
- Não usa boto3.

## S3ObjectStore (R2)

- Mesma semântica já validada: PUT/multipart; chave final só após complete; abort de
  multipart incompleto.
- Endpoint típico R2: `https://<accountid>.r2.cloudflarestorage.com`.
- Bucket privado; nenhum objeto desta feature é configurado como público no código.

## Fake de teste

Implementa o Protocolo em memória (dict `key → bytes`). Testes de domínio **não** dependem
de boto3 nem de disco real, salvo os testes unitários do próprio `FilesystemObjectStore`.
