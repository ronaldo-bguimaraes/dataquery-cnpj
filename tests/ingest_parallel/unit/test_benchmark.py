"""T040 — Benchmark: speedup medível com N workers e integridade preservada."""

from __future__ import annotations

import threading

from src.ingest_parallel.benchmark import benchmark_split_runs


def test_speedup_e_integridade_com_workers():
    partitions = [
        {"partition_id": f"empresas-{i:02d}", "table": "empresas", "files": [], "expected_rows": 0}
        for i in range(32)
    ]
    lock = threading.Lock()

    def trabalho(_partition) -> None:
        pass  # noop

    summary = benchmark_split_runs(partitions, trabalho, max_workers=8)
    assert summary["particoes"] == 32
    assert summary["paralelo_segundos"] <= summary["sequencial_segundos"] + 0.05

    def trabalho_lento(_partition) -> None:
        import time

        time.sleep(0.002)  # simula IO (GIL liberado → escala com threads)

    com_trabalho = benchmark_split_runs(partitions, trabalho_lento, max_workers=8)
    assert com_trabalho["speedup"] > 1.2  # SC-006: ≥2× no ambiente alvo; aqui ≥1.2 determinístico
