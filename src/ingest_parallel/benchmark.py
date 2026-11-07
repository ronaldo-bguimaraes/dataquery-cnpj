"""Benchmark de duração/comparativo 1 worker vs N workers (SC-006, T040)."""

from __future__ import annotations

import concurrent.futures
import time
from typing import Any


def benchmark_split_runs(
    partitions: list[dict],
    work,
    *,
    max_workers: int = 8,
) -> dict[str, Any]:
    """Mede duração em dois modos: sequencial (baseline) e paralelo (N workers).

    Retorna contagens e durações para comparação de speedup; a integridade
    (mesmas partições processadas em ambos os modos) é verificada pelo caller.
    """

    def _run(sequential: bool) -> tuple[list[str], float]:
        started = time.monotonic()
        concluidas: list[str] = []
        if sequential:
            for partition in partitions:
                work(partition)
                concluidas.append(partition["partition_id"])
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
                list(pool.map(work, partitions))
                concluidas = [p["partition_id"] for p in partitions]
        return concluidas, time.monotonic() - started

    sequencial, dur_seq = _run(True)
    paralelo, dur_par = _run(False)
    assert sorted(sequencial) == sorted(paralelo), "integridade: conjuntos divergem"
    speedup = dur_seq / dur_par if dur_par > 0 else 1.0
    return {
        "particoes": len(partitions),
        "max_workers": max_workers,
        "sequencial_segundos": round(dur_seq, 6),
        "paralelo_segundos": round(dur_par, 6),
        "speedup": round(speedup, 3),
    }
