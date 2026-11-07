"""T005 — Dataset Identity Registry e chaves de negócio (FR-003, FR-004)."""

from __future__ import annotations

import pytest

from src.ingest_parallel.contracts import DATASET_REGISTRY, DatasetIdentity, identity_key


class TestDatasetIdentityRegistry:
    def test_quatro_datasets_declaram_identidade(self):
        assert set(DATASET_REGISTRY) == {
            "empresas",
            "estabelecimentos",
            "socios",
            "simples",
        }

    def test_business_keys_conforme_data_model(self):
        registro = DATASET_REGISTRY
        assert registro["empresas"].business_keys == ("cnpj_basico",)
        assert registro["estabelecimentos"].business_keys == ("cnpj_completo",)
        assert registro["simples"].business_keys == ("cnpj_basico",)
        assert registro["socios"].business_keys == (
            "cnpj_basico",
            "identificador_socio",
            "documento_socio_mascarado",
            "nome_socio",
            "qualificacao_socio_codigo",
            "data_entrada_sociedade",
        )

    def test_todos_tem_politica_dedup_e_pii(self):
        for dataset, identity in DATASET_REGISTRY.items():
            assert isinstance(identity, DatasetIdentity)
            assert identity.dedup_policy in {"keep_first", "keep_last", "merge", "reject"}
            assert identity.pii_policy in {"mask", "omit", "preserve"}
            assert len(identity.business_keys) >= 1

    def test_socio_tem_pii_politica_mask(self):
        assert DATASET_REGISTRY["socios"].pii_policy == "mask"


class TestIdentityKey:
    def test_key_empresas(self):
        row = {"cnpj_basico": "12345678"}
        assert identity_key("empresas", row) == ("12345678",)

    def test_key_socios_ignora_dados_que_nao_sao_da_chave(self):
        row = {
            "cnpj_basico": "12345678",
            "identificador_socio": "1",
            "documento_socio_mascarado": "***",
            "nome_socio": "JOÃO DA SILVA",
            "qualificacao_socio_codigo": "10",
            "data_entrada_sociedade": "2020-01-01",
            "capital_social": "999",  # campo alheio à chave
        }
        assert identity_key("socios", row) == (
            "12345678",
            "1",
            "***",
            "JOÃO DA SILVA",
            "10",
            "2020-01-01",
        )

    def test_key_normalizada_para_none(self):
        row = {"cnpj_basico": None}
        assert identity_key("simples", row) == (None,)

    def test_dataset_desconhecido_rejeita(self):
        with pytest.raises(KeyError):
            identity_key("inexistente", {})
