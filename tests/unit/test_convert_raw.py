from __future__ import annotations

import importlib
import json
from pathlib import Path

import pyarrow as pa
import pytest

import src.conversao.bronze as bronze_module
from src.conversao.bronze import convert_raw_file
from src.extracao_rfb.competencia import InvalidCompetenciaError
from src.storage.minio import S3OperationError
from tests.fakes import FakeObjectStore, make_zip, read_parquet

CSV = "1111-1/00;Comércio\n2222-2/00;Serviços\n".encode("latin1")
RAW_KEY = "raw/2026-02/Cnaes.zip"
BRONZE_KEY = "bronze/2026-02/Cnaes.parquet"


def test_converts_single_csv_to_bronze_parquet(tmp_path):
    store = FakeObjectStore({RAW_KEY: make_zip({"Cnaes.csv": CSV})})

    result = convert_raw_file(
        "2026-02",
        "Cnaes.zip",
        RAW_KEY,
        store=store,
        scratch_dir=tmp_path,
    )

    assert result.status == "publicado"
    assert result.nome == "Cnaes.zip"
    assert result.raw_key == RAW_KEY
    assert result.bronze_key == BRONZE_KEY
    assert result.linhas == 2
    assert result.bytes_parquet > 0
    assert result.erro is None
    assert store.get_calls == [RAW_KEY]
    assert store.put_calls == [BRONZE_KEY]
    assert list(tmp_path.iterdir()) == []


def test_bronze_parquet_uses_central_schema_and_preserves_text(tmp_path):
    store = FakeObjectStore({RAW_KEY: make_zip({"Cnaes.csv": CSV})})

    result = convert_raw_file(
        "2026-02", "Cnaes.zip", RAW_KEY, store=store, scratch_dir=tmp_path
    )

    table = read_parquet(store.objects[result.bronze_key])

    assert table.num_rows == 2
    assert table.column_names == ["cnae_codigo", "cnae_descricao"]
    assert all(pa.types.is_string(field.type) for field in table.schema)
    assert table.to_pydict() == {
        "cnae_codigo": ["1111-1/00", "2222-2/00"],
        "cnae_descricao": ["Comércio", "Serviços"],
    }


def test_conversion_uses_block_reading_with_latin1_and_semicolon(tmp_path, monkeypatch):
    store = FakeObjectStore({RAW_KEY: make_zip({"Cnaes.csv": CSV})})
    calls: dict = {}
    original = bronze_module.convert_csv_to_parquet

    def spy(*args, **kwargs):
        calls.update(kwargs)
        return original(*args, **kwargs)

    monkeypatch.setattr(bronze_module, "convert_csv_to_parquet", spy)

    result = convert_raw_file(
        "2026-02",
        "Cnaes.zip",
        RAW_KEY,
        store=store,
        scratch_dir=tmp_path,
        chunk_size=1024,
    )

    assert result.status == "publicado"
    assert calls["encoding"] == "latin1"
    assert calls["delimiter"] == ";"
    assert calls["chunk_size"] == 1024


def test_zip_with_zero_members_fails_without_bronze(tmp_path):
    store = FakeObjectStore({RAW_KEY: make_zip({})})

    result = convert_raw_file(
        "2026-02", "Cnaes.zip", RAW_KEY, store=store, scratch_dir=tmp_path
    )

    assert result.status == "falhou"
    assert result.linhas is None
    assert result.bronze_key == BRONZE_KEY
    assert BRONZE_KEY not in store.objects
    assert result.erro
    assert list(tmp_path.iterdir()) == []


def test_zip_with_multiple_members_fails(tmp_path):
    store = FakeObjectStore(
        {RAW_KEY: make_zip({"Cnaes.csv": CSV, "Outro.csv": CSV})}
    )

    result = convert_raw_file(
        "2026-02", "Cnaes.zip", RAW_KEY, store=store, scratch_dir=tmp_path
    )

    assert result.status == "falhou"
    assert BRONZE_KEY not in store.objects
    assert "ZIP" in (result.erro or "") or "CSV" in (result.erro or "")


def test_unknown_schema_fails(tmp_path):
    store = FakeObjectStore({RAW_KEY: make_zip({"Cnaes.csv": CSV})})

    result = convert_raw_file(
        "2026-02", "Desconhecido.zip", RAW_KEY, store=store, scratch_dir=tmp_path
    )

    assert result.status == "falhou"
    assert "schema" in (result.erro or "").lower()
    assert "bronze/2026-02/Desconhecido.parquet" not in store.objects


def test_invalid_csv_fails_and_preserves_previous_bronze(tmp_path, monkeypatch):
    store = FakeObjectStore({RAW_KEY: make_zip({"Cnaes.csv": CSV})})
    store.objects[BRONZE_KEY] = b"previous-valid-parquet"

    def broken_conversion(*args, **kwargs):
        raise pa.ArrowInvalid("CSV inválido")

    monkeypatch.setattr(bronze_module, "convert_csv_to_parquet", broken_conversion)

    result = convert_raw_file(
        "2026-02", "Cnaes.zip", RAW_KEY, store=store, scratch_dir=tmp_path
    )

    assert result.status == "falhou"
    assert "CSV" in (result.erro or "") or "ArrowInvalid" in (result.erro or "")
    assert store.objects[BRONZE_KEY] == b"previous-valid-parquet"
    assert list(tmp_path.iterdir()) == []


def test_success_replaces_previous_bronze_only_after_upload(tmp_path):
    store = FakeObjectStore({RAW_KEY: make_zip({"Cnaes.csv": CSV})})
    store.objects[BRONZE_KEY] = b"previous-valid-parquet"

    result = convert_raw_file(
        "2026-02", "Cnaes.zip", RAW_KEY, store=store, scratch_dir=tmp_path
    )

    assert result.status == "publicado"
    assert store.objects[BRONZE_KEY] != b"previous-valid-parquet"
    assert store.objects[BRONZE_KEY].startswith(b"PAR1")


def test_upload_failure_preserves_previous_bronze_and_removes_scratch(tmp_path):
    store = FakeObjectStore({RAW_KEY: make_zip({"Cnaes.csv": CSV})})
    store.objects[BRONZE_KEY] = b"previous-valid-parquet"
    store.fail_put = S3OperationError("falha simulada no upload")

    result = convert_raw_file(
        "2026-02", "Cnaes.zip", RAW_KEY, store=store, scratch_dir=tmp_path
    )

    assert result.status == "falhou"
    assert store.objects[BRONZE_KEY] == b"previous-valid-parquet"
    assert list(tmp_path.iterdir()) == []


def test_missing_raw_key_fails_without_bronze(tmp_path):
    store = FakeObjectStore()

    result = convert_raw_file(
        "2026-02", "Cnaes.zip", RAW_KEY, store=store, scratch_dir=tmp_path
    )

    assert result.status == "falhou"
    assert BRONZE_KEY not in store.objects
    assert list(tmp_path.iterdir()) == []


def test_invalid_competencia_fails_before_storage_access(tmp_path):
    store = FakeObjectStore({RAW_KEY: make_zip({"Cnaes.csv": CSV})})

    with pytest.raises(InvalidCompetenciaError):
        convert_raw_file(
            "2026-13", "Cnaes.zip", RAW_KEY, store=store, scratch_dir=tmp_path
        )

    assert store.get_calls == []
    assert store.put_calls == []


def test_result_is_json_serializable_without_urls_or_content(tmp_path):
    store = FakeObjectStore({RAW_KEY: make_zip({"Cnaes.csv": CSV})})

    result = convert_raw_file(
        "2026-02", "Cnaes.zip", RAW_KEY, store=store, scratch_dir=tmp_path
    )
    payload = json.dumps(result.to_dict())

    assert "http://" not in payload
    assert "https://" not in payload
    assert "Comércio" not in payload


def test_importing_read_files_does_not_convert(monkeypatch):
    read_files = importlib.import_module("read_files")

    def fail_conversion(*args, **kwargs):
        raise AssertionError("conversão não pode rodar no import")

    monkeypatch.setattr(
        read_files, "convert_compressed_csv_to_parquet", fail_conversion
    )

    reloaded = importlib.reload(read_files)

    assert hasattr(reloaded, "convert_zip")
    source = Path(__file__).resolve().parents[2] / "read_files.py"
    assert "process = Process()" not in source.read_text()


def test_conversion_error_does_not_leak_csv_values_or_cpf(tmp_path, monkeypatch):
    store = FakeObjectStore({RAW_KEY: make_zip({"Cnaes.csv": CSV})})

    def broken_conversion(*args, **kwargs):
        raise pa.ArrowInvalid(
            "CSV parse error near '123.456.789-00'; expected string"
        )

    monkeypatch.setattr(bronze_module, "convert_csv_to_parquet", broken_conversion)

    result = convert_raw_file(
        "2026-02", "Cnaes.zip", RAW_KEY, store=store, scratch_dir=tmp_path
    )

    assert result.status == "falhou"
    assert "123.456.789-00" not in (result.erro or "")
    assert "ArrowInvalid" in (result.erro or "")
    assert list(tmp_path.iterdir()) == []


def _filesystem_store_with_raw(tmp_path):
    from src.storage.filesystem import FilesystemObjectStore

    root = tmp_path / "data"
    raw_path = root / RAW_KEY
    raw_path.parent.mkdir(parents=True)
    raw_path.write_bytes(make_zip({"Cnaes.csv": CSV}))
    return FilesystemObjectStore(root=root), root


def test_filesystem_store_retry_rebuilds_bronze_without_redownload(tmp_path):
    store, root = _filesystem_store_with_raw(tmp_path)
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    first = convert_raw_file(
        "2026-02", "Cnaes.zip", RAW_KEY, store=store, scratch_dir=scratch
    )
    first_bytes = (root / BRONZE_KEY).read_bytes()
    second = convert_raw_file(
        "2026-02", "Cnaes.zip", RAW_KEY, store=store, scratch_dir=scratch
    )

    assert first.status == "publicado"
    assert second.status == "publicado"
    assert (root / BRONZE_KEY).read_bytes().startswith(b"PAR1")
    assert first_bytes.startswith(b"PAR1")
    assert read_parquet((root / BRONZE_KEY).read_bytes()).num_rows == 2
    assert list(scratch.iterdir()) == []
    assert not list(root.rglob("*.part"))


def test_filesystem_store_upload_failure_preserves_previous_bronze(
    tmp_path, monkeypatch
):
    import src.storage.filesystem as filesystem_module

    store, root = _filesystem_store_with_raw(tmp_path)
    bronze_path = root / BRONZE_KEY
    bronze_path.parent.mkdir(parents=True)
    bronze_path.write_bytes(b"previous-valid-parquet")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    original = filesystem_module._copy_file

    def flaky_copy(source, destination, *, chunk_size=1024):
        from pathlib import Path

        if ".parquet" in Path(destination).name:
            Path(destination).write_bytes(b"partial")
            raise OSError("disco cheio")
        return original(source, destination, chunk_size=chunk_size)

    monkeypatch.setattr(filesystem_module, "_copy_file", flaky_copy)

    result = convert_raw_file(
        "2026-02", "Cnaes.zip", RAW_KEY, store=store, scratch_dir=scratch
    )

    assert result.status == "falhou"
    assert bronze_path.read_bytes() == b"previous-valid-parquet"
    assert list(scratch.iterdir()) == []
    assert not list(root.rglob("*.part"))


def test_filesystem_store_success_replaces_bronze_and_cleans_scratch(tmp_path):
    store, root = _filesystem_store_with_raw(tmp_path)
    bronze_path = root / BRONZE_KEY
    bronze_path.parent.mkdir(parents=True)
    bronze_path.write_bytes(b"previous-valid-parquet")
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    result = convert_raw_file(
        "2026-02", "Cnaes.zip", RAW_KEY, store=store, scratch_dir=scratch
    )

    assert result.status == "publicado"
    assert bronze_path.read_bytes().startswith(b"PAR1")
    assert list(scratch.iterdir()) == []
    assert not list(root.rglob("*.part"))
