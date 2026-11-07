"""T046 — Manutenção: compaction/expire stage observável e sem comprometer a versão ativa (FR-036/048)."""

from __future__ import annotations

from src.ingest_parallel.maintenance import build_maintenance_plan, validate_maintenance_safety


class TestPlanoManutencao:
    def test_plano_inclui_compaction_e_expiracao(self):
        plan = build_maintenance_plan(
            ["silver_empresas", "silver_estabelecimentos"],
            retention_versions=2,
        )
        acoes = {item["acao"] for item in plan["passos"]}
        assert "compact" in acoes
        assert "expire_snapshots" in acoes

    def test_plano_preserva_versao_ativa(self):
        plan = build_maintenance_plan(["silver_empresas"], retention_versions=2)
        snapshot_ativa = "snap-1"
        assert validate_maintenance_safety(plan, active_snapshot=snapshot_ativa) is True
        com_zero_retention = {
            "tabelas": ["silver_empresas"],
            "passo_ativo_preservado": True,
            "passos": [{"acao": "expire_snapshots", "parametros": {"manter_ultimas": 0}}],
        }
        assert validate_maintenance_safety(com_zero_retention, active_snapshot="snap-9") is False

    def test_retengao_minima_versao_anterior(self):
        plan = build_maintenance_plan(["silver_empresas"], retention_versions=2)
        expire = next(p for p in plan["passos"] if p["acao"] == "expire_snapshots")
        assert expire["parametros"]["manter_ultimas"] >= 2

    def test_retention_zero_expiraria_versao_ativa(self):
        plan_inseguro = {
            "tabelas": ["silver_empresas"],
            "passo_ativo_preservado": True,
            "passos": [
                {"acao": "expire_snapshots", "tabelas": ["silver_empresas"], "parametros": {"manter_ultimas": 0}}
            ],
        }
        assert validate_maintenance_safety(plan_inseguro, active_snapshot="snap-9") is False
