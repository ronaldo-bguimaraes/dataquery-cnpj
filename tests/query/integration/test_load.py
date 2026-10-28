from __future__ import annotations

from pathlib import Path

from src.query.ingest.load import load_inventory


def test_load_publishes_counts_and_metadata(query_db, source_dir: Path, query_dsn: str):
    summary = load_inventory(source_dir, query_dsn, competencia="2026-10")

    assert summary["competencia"] == "2026-10"
    assert summary["empresas"] == 3
    assert summary["estabelecimentos"] == 4
    assert summary["socios"] == 2
    assert query_db.execute("SELECT count(*) FROM empresas").fetchone()[0] == 3
    assert query_db.execute("SELECT count(*) FROM estabelecimentos").fetchone()[0] == 4
    assert query_db.execute("SELECT count(*) FROM estabelecimentos_listagem").fetchone()[0] == 4
    assert query_db.execute("SELECT count(*) FROM estabelecimento_cnaes").fetchone()[0] == 6
    metadata = query_db.execute(
        "SELECT competencia, fonte, total_empresas, total_estabelecimentos, total_socios "
        "FROM query_metadata WHERE ativo"
    ).fetchone()
    assert metadata == ("2026-10", "Receita Federal", 3, 4, 2)


def test_load_derives_cnpj_cnaes_and_optantes(query_db, source_dir: Path, query_dsn: str):
    load_inventory(source_dir, query_dsn, competencia="2026-10")

    row = query_db.execute(
        "SELECT cnpj_completo FROM estabelecimentos WHERE cnpj_basico = '00000001' "
        "ORDER BY cnpj_ordem LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row[0] == "00000001000136"

    codes = query_db.execute(
        "SELECT cnae_codigo, principal FROM estabelecimento_cnaes "
        "WHERE cnpj_completo = '00000001000136' ORDER BY cnae_codigo"
    ).fetchall()
    assert codes == [("1111301", True), ("2222201", False), ("3333301", False)]

    optantes = query_db.execute(
        "SELECT cnpj_basico, optante_simples, optante_mei FROM simples ORDER BY cnpj_basico"
    ).fetchall()
    assert optantes == [
        ("00000001", True, False),
        ("00000002", False, True),
    ]


def test_load_masks_cpf(query_db, source_dir: Path, query_dsn: str):
    load_inventory(source_dir, query_dsn, competencia="2026-10")

    documents = query_db.execute(
        "SELECT documento_socio_mascarado FROM socios ORDER BY cnpj_basico"
    ).fetchall()
    assert len(documents) == 2
    assert all(value == "***.***.***-**" for (value,) in documents)
    assert query_db.execute(
        "SELECT count(*) FROM socios WHERE documento_socio_mascarado ~ '^[0-9]{11}$'"
    ).fetchone()[0] == 0


def test_reload_is_idempotent(query_db, source_dir: Path, query_dsn: str):
    first = load_inventory(source_dir, query_dsn, competencia="2026-10")
    second = load_inventory(source_dir, query_dsn, competencia="2026-10")

    assert first["empresas"] == second["empresas"] == 3
    assert query_db.execute("SELECT count(*) FROM empresas").fetchone()[0] == 3
    assert query_db.execute("SELECT count(*) FROM query_metadata WHERE ativo").fetchone()[0] == 1
