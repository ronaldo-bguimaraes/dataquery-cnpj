"""Opt-in integration test against the real RFB WebDAV.

Run with::

    docker compose --profile test run --rm -e RUN_NETWORK_TESTS=1 airflow-tests \\
      tests/integration -q

The default test suite skips this module.
"""

from __future__ import annotations

import os

import pytest

from src.extracao_rfb.rfb_client import ClientRFB

pytestmark = [
    pytest.mark.network,
    pytest.mark.skipif(
        os.environ.get("RUN_NETWORK_TESTS") != "1",
        reason="defina RUN_NETWORK_TESTS=1 para acessar a RFB",
    ),
]


def test_real_competencia_listing_is_sorted_and_has_no_directories():
    files = ClientRFB().list_files("2026-02")

    assert len(files) == 37
    assert files == sorted(files)
    assert all(name and not name.endswith("/") for name in files)
