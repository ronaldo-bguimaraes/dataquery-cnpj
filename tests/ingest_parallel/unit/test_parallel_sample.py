"""T034 — Execução paralela no sample com teto de concorrência respeitado (SC-005)."""

from __future__ import annotations

import threading

from src.ingest_parallel.orchestrate import run_parallel


class TestParalelaSample:
    def test_8_workers_maximo_e_todas_particoes_processadas(self):
        partitions = [
            {"partition_id": f"empresas-{i:02d}", "table": "empresas", "files": [], "expected_rows": 0}
            for i in range(16)
        ]
        lock = threading.Lock()
        ativos = {"atual": 0, "maximo": 0}
        executadas: list[str] = []

        def worker(partition) -> None:
            with lock:
                ativos["atual"] += 1
                ativos["maximo"] = max(ativos["maximo"], ativos["atual"])
            executadas.append(partition["partition_id"])
            with lock:
                ativos["atual"] -= 1

        run_parallel(partitions, worker, max_workers=8)

        assert sorted(executadas) == sorted(p["partition_id"] for p in partitions)
        assert ativos["maximo"] <= 8  # FR-023 / SC-005

    def test_ordem_independente_e_dados_determinados(self):
        partitions = [
            {"partition_id": f"t-{i:02d}", "table": "empresas", "files": [], "expected_rows": 0}
            for i in range(5)
        ]
        resultados: dict[str, str] = {}

        run_parallel(partitions, lambda p: resultados.setdefault(p["partition_id"], "ok"), max_workers=4)
        assert len(resultados) == 5

    def test_falha_em_worker_propaga_sem_perder_o_resto(self):
        partitions = [
            {"partition_id": f"t-{i}", "table": "empresas", "files": [], "expected_rows": 0}
            for i in range(4)
        ]

        concluidas: list[str] = []
        erros: list[str] = []

        def worker(partition, concluidas=concluidas, erros=erros):  # noqa: ANN001
            if partition["partition_id"] == "t-1":
                raise RuntimeError("worker morto")
            concluidas.append(partition["partition_id"])

        run_parallel(partitions, worker, max_workers=4, on_error=erros.append)

        assert sorted(concluidas) == ["t-0", "t-2", "t-3"]
        assert erros == ["t-1"]
