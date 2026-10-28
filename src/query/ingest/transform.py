"""Transform Bronze Parquet rows into the PostgreSQL serving model."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import pyarrow.parquet as pq

from src.query.domain.masking import mask_document
from src.query.domain.normalization import (
    build_cnpj,
    competence_end_date,
    is_optante,
    normalize_cnpj,
    normalize_text,
    parse_date,
    parse_decimal_br,
    split_cnaes,
)


@dataclass
class TransformStats:
    invalid_cnpj: int = 0
    invalid_date: int = 0
    invalid_capital: int = 0
    invalid_cnae: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "invalid_cnpj": self.invalid_cnpj,
            "invalid_date": self.invalid_date,
            "invalid_capital": self.invalid_capital,
            "invalid_cnae": self.invalid_cnae,
        }


def _rows(paths: list[Path]) -> Iterator[dict[str, Any]]:
    for path in paths:
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(batch_size=50_000):
            yield from batch.to_pylist()


def _text(row: dict[str, Any], key: str) -> str:
    return str(row.get(key) or "").strip().strip('"')


def empresas_rows(paths: list[Path], stats: TransformStats) -> Iterator[dict[str, Any]]:
    for row in _rows(paths):
        basico = normalize_cnpj(_text(row, "empresa_cnpj_basico"))
        if len(basico) != 8:
            stats.invalid_cnpj += 1
            continue
        capital_raw = _text(row, "empresa_capital_social")
        capital = parse_decimal_br(capital_raw)
        if capital is None and capital_raw:
            stats.invalid_capital += 1
        razao = _text(row, "empresa_razao_social")
        yield {
            "cnpj_basico": basico,
            "razao_social": razao,
            "razao_social_normalizada": normalize_text(razao),
            "natureza_juridica_codigo": _text(row, "empresa_natureza_juridica"),
            "qualificacao_responsavel_codigo": _text(
                row, "empresa_qualificacao_responsavel"
            ),
            "capital_social": capital,
            "porte_codigo": _text(row, "empresa_porte"),
            "ente_federativo": _text(row, "empresa_ente_federativo"),
        }


def estabelecimentos_rows(
    paths: list[Path], stats: TransformStats
) -> Iterator[tuple[dict[str, Any], list[tuple[str, str, bool]]]]:
    for row in _rows(paths):
        basico = normalize_cnpj(_text(row, "estabelecimento_cnpj_basico"))
        ordem = normalize_cnpj(_text(row, "estabelecimento_cnpj_ordem"))
        dv = normalize_cnpj(_text(row, "estabelecimento_cnpj_dv"))
        try:
            cnpj_completo = build_cnpj(basico, ordem, dv)
        except ValueError:
            stats.invalid_cnpj += 1
            continue

        principal = normalize_cnpj(_text(row, "estabelecimento_cnae_fiscal_principal"))
        if principal and len(principal) != 7:
            stats.invalid_cnae += 1
            principal = ""
        secondary = split_cnaes(_text(row, "estabelecimento_cnae_fiscal_secundaria"))
        cnaes: list[tuple[str, str, bool]] = []
        seen: set[str] = set()
        if principal:
            cnaes.append((cnpj_completo, principal, True))
            seen.add(principal)
        for code in secondary:
            if code not in seen:
                cnaes.append((cnpj_completo, code, False))
                seen.add(code)

        data_situacao = parse_date(_text(row, "estabelecimento_data_situacao_cadastral"))
        data_abertura = parse_date(_text(row, "estabelecimento_data_inicio_atividade"))
        data_especial = parse_date(_text(row, "estabelecimento_data_situacao_especial"))
        for raw_date in (
            _text(row, "estabelecimento_data_situacao_cadastral"),
            _text(row, "estabelecimento_data_inicio_atividade"),
            _text(row, "estabelecimento_data_situacao_especial"),
        ):
            if raw_date and raw_date not in {"00000000", "0"}:
                if parse_date(raw_date) is None:
                    stats.invalid_date += 1

        yield (
            {
                "cnpj_completo": cnpj_completo,
                "cnpj_basico": basico,
                "cnpj_ordem": ordem,
                "cnpj_dv": dv,
                "identificador_matriz_filial": _text(
                    row, "estabelecimento_identificador_matriz_filial"
                ),
                "nome_fantasia": _text(row, "estabelecimento_nome_fantasia"),
                "situacao_cadastral": _text(
                    row, "estabelecimento_situacao_cadastral"
                ),
                "data_situacao_cadastral": data_situacao,
                "motivo_situacao_cadastral": _text(
                    row, "estabelecimento_motivo_situacao_cadastral"
                ),
                "nome_cidade_exterior": _text(
                    row, "estabelecimento_nome_cidade_exterior"
                ),
                "pais_codigo": _text(row, "estabelecimento_pais"),
                "data_inicio_atividade": data_abertura,
                "cnae_principal": principal,
                "cnae_secundaria_texto": _text(
                    row, "estabelecimento_cnae_fiscal_secundaria"
                ),
                "tipo_logradouro": _text(row, "estabelecimento_tipo_logradouro"),
                "logradouro": _text(row, "estabelecimento_logradouro"),
                "numero": _text(row, "estabelecimento_numero"),
                "complemento": _text(row, "estabelecimento_complemento"),
                "bairro": _text(row, "estabelecimento_bairro"),
                "cep": _text(row, "estabelecimento_cep"),
                "uf": _text(row, "estabelecimento_uf").upper(),
                "municipio_codigo": _text(row, "estabelecimento_municipio"),
                "ddd_1": _text(row, "estabelecimento_ddd_1"),
                "telefone_1": _text(row, "estabelecimento_telefone_1"),
                "ddd_2": _text(row, "estabelecimento_ddd_2"),
                "telefone_2": _text(row, "estabelecimento_telefone_2"),
                "ddd_fax": _text(row, "estabelecimento_ddd_fax"),
                "numero_fax": _text(row, "estabelecimento_numero_fax"),
                "correio_eletronico": _text(
                    row, "estabelecimento_correio_eletronico"
                ),
                "situacao_especial": _text(
                    row, "estabelecimento_situacao_especial"
                ),
                "data_situacao_especial": data_especial,
            },
            cnaes,
        )


def socios_rows(paths: list[Path], stats: TransformStats) -> Iterator[dict[str, Any]]:
    for row in _rows(paths):
        basico = normalize_cnpj(_text(row, "socio_cnpj_basico"))
        if len(basico) != 8:
            stats.invalid_cnpj += 1
            continue
        yield {
            "cnpj_basico": basico,
            "identificador_socio": _text(row, "socio_identificador_socio"),
            "nome_socio": _text(row, "socio_nome_socio"),
            "documento_socio_mascarado": mask_document(
                _text(row, "socio_cnpj_cpf_socio")
            ),
            "qualificacao_socio_codigo": _text(row, "socio_qualificacao_socio"),
            "data_entrada_sociedade": parse_date(
                _text(row, "socio_data_entrada_sociedade")
            ),
            "pais_codigo": _text(row, "socio_pais"),
            "representante_legal": mask_document(_text(row, "socio_representante_legal")),
            "nome_representante": _text(row, "socio_nome_representante"),
            "qualificacao_representante_legal": _text(
                row, "socio_qualificacao_representante_legal"
            ),
            "faixa_etaria": _text(row, "socio_faixa_etaria"),
        }


def simples_rows(
    path: Path, competencia: str, stats: TransformStats
) -> Iterator[dict[str, Any]]:
    reference = competence_end_date(competencia)
    for row in _rows([path]):
        basico = normalize_cnpj(_text(row, "simples_cnpj_basico"))
        if len(basico) != 8:
            stats.invalid_cnpj += 1
            continue
        flag_simples = _text(row, "simples_opcao_simples")
        flag_mei = _text(row, "simples_opcao_mei")
        exclusao_simples = parse_date(_text(row, "simples_data_exclusao_simples"))
        exclusao_mei = parse_date(_text(row, "simples_data_exclusao_mei"))
        yield {
            "cnpj_basico": basico,
            "opcao_simples": flag_simples,
            "data_opcao_simples": parse_date(
                _text(row, "simples_data_opcao_simples")
            ),
            "data_exclusao_simples": exclusao_simples,
            "opcao_mei": flag_mei,
            "data_opcao_mei": parse_date(_text(row, "simples_data_opcao_mei")),
            "data_exclusao_mei": exclusao_mei,
            "optante_simples": is_optante(flag_simples, exclusao_simples, reference),
            "optante_mei": is_optante(flag_mei, exclusao_mei, reference),
        }


def lookup_rows(
    path: Path, code_column: str, description_column: str
) -> Iterator[tuple[str, str]]:
    for row in _rows([path]):
        code = _text(row, code_column)
        if not code:
            continue
        yield code, _text(row, description_column)
