from __future__ import annotations

import importlib.util
from pathlib import Path

DAG_PATH = Path(__file__).resolve().parents[2] / "airflow" / "dags" / "cnpj_download.py"


def load_dag_module():
    spec = importlib.util.spec_from_file_location("cnpj_download_dag", DAG_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_dag_import_does_not_touch_network(block_network):
    module = load_dag_module()
    dag = module.dag

    assert dag.dag_id == "cnpj_download_manual"
    assert dag.schedule is None
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.task_ids == ["baixar_arquivos"]
    assert {"cnpj", "download", "manual"} <= set(dag.tags)
    assert "competencia" in dag.params


def test_limit_param_schema():
    module = load_dag_module()
    limite = module.dag.params.get_param("limite")

    assert limite.schema == {"type": ["null", "integer"], "minimum": 1}


def test_task_forwards_competencia_and_limite(monkeypatch, tmp_path):
    module = load_dag_module()

    calls = {}

    class FakeSummary:
        def to_dict(self):
            return {"competencia": "2026-02", "parcial": True}

    def fake_download_competencia(**kwargs):
        calls.update(kwargs)
        return FakeSummary()

    import src.extracao_rfb.download_competencia as domain

    monkeypatch.setattr(domain, "download_competencia", fake_download_competencia)
    monkeypatch.setenv("CNPJ_DOWNLOAD_DIR", str(tmp_path))

    task = module.dag.get_task("baixar_arquivos")
    result = task.python_callable(
        params={"competencia": "2026-02", "limite": 1}
    )

    assert calls == {
        "competencia": "2026-02",
        "output_dir": str(tmp_path),
        "limite": 1,
    }
    assert result == {"competencia": "2026-02", "parcial": True}
