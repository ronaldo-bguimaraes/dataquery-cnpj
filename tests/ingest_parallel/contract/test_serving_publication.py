"""T016 — Contrato de publicação do serving (contracts/serving_publication.md)."""

from __future__ import annotations

import pytest

from src.ingest_parallel.publish import (
    PublicationState,
    staging_table_name,
    swap_plan,
)


class TestStaging:
    def test_nomme_de_staging_por_load_id(self):
        assert (
            staging_table_name("empresas", "2026-10-aaaaaaaa")
            == "staging_empresas_2026-10-aaaaaaaa"
        )

    def test_staging_nunca_colide_com_base(self):
        for table in ("empresas", "estabelecimentos", "socios", "simples"):
            name = staging_table_name(table, "2026-10-aaaa")
            assert name != table
            assert name.startswith("staging_")


class TestSwapPlan:
    def test_todas_as_tabelas_no_plano(self):
        plan = swap_plan(
            load_id="2026-10-aaaa",
            tables=("empresas", "estabelecimentos", "socios", "simples", "query_metadata"),
            previous_active="2026-05-bbbb",
        )
        assert isinstance(plan, PublicationState)
        assert plan.load_id == "2026-10-aaaa"
        assert plan.previous_active == "2026-05-bbbb"
        assert len(plan.swaps) == 4 + 1  # 4 tabelas + rotation de metadata
        assert plan.swaps[-1].target == "query_metadata"

    def test_swap_tem_staging_e_target(self):
        plan = swap_plan(
            load_id="x",
            tables=("empresas",),
            previous_active="antigo",
        )
        swap = plan.swaps[0]
        assert swap.staging == staging_table_name("empresas", "x")
        assert swap.target == "empresas"
        assert swap.staging != swap.target

    def test_estado_publicado_e_retorno(self):
        plan = swap_plan(load_id="x", tables=("empresas",), previous_active="antigo")
        assert plan.status == "planned"
        assert plan.rollback_to == "antigo"

    def test_swap_valida_lock_timeout(self):
        plan = swap_plan(load_id="x", tables=("empresas",), previous_active="antigo")
        assert plan.lock_timeout == "2s"
