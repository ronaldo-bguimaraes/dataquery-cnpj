"""Small HTTP benchmark for the listing and detail pages."""

from __future__ import annotations

import argparse
import json
import re
import statistics
import time
from typing import Any

import httpx

CNPJ_HREF = re.compile(r'/estabelecimentos/(\d{14})')


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round(len(ordered) * 0.95)) - 1))
    return ordered[index]


def _measure(
    client: httpx.Client, url: str, count: int, params: dict[str, str] | None = None
) -> list[float]:
    durations: list[float] = []
    for _ in range(count):
        started = time.perf_counter()
        response = client.get(url, params=params)
        response.raise_for_status()
        durations.append((time.perf_counter() - started) * 1000)
    return durations


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Mede p95 da listagem e do detalhe.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--requisicoes", type=int, default=20)
    parser.add_argument("--uf", default="")
    parser.add_argument("--cnae", default="")
    args = parser.parse_args(argv)
    if args.requisicoes < 1:
        parser.error("--requisicoes deve ser >= 1")

    params = {}
    if args.uf:
        params["uf"] = args.uf
    if args.cnae:
        params["cnae"] = args.cnae
    listing_url = f"{args.base_url.rstrip('/')}/estabelecimentos"
    result: dict[str, Any] = {"listing_requests": args.requisicoes}

    try:
        with httpx.Client(timeout=30) as client:
            first = client.get(listing_url, params=params)
            first.raise_for_status()
            match = CNPJ_HREF.search(first.text)
            if match:
                detail_url = f"{args.base_url.rstrip('/')}/estabelecimentos/{match.group(1)}"
                detail_durations = _measure(client, detail_url, args.requisicoes)
                result["detail_requests"] = args.requisicoes
                result["detail_p95_ms"] = round(_p95(detail_durations), 2)
                result["detail_median_ms"] = round(statistics.median(detail_durations), 2)
            else:
                result["detail_requests"] = 0
                result["detail_error"] = "nenhum CNPJ encontrado na listagem"

            listing_durations = _measure(client, listing_url, args.requisicoes, params)
            result["listing_p95_ms"] = round(_p95(listing_durations), 2)
            result["listing_median_ms"] = round(statistics.median(listing_durations), 2)
    except Exception as exc:  # noqa: BLE001 - CLI reports the failure
        print(json.dumps({"error": type(exc).__name__}, ensure_ascii=False))
        return 1

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
