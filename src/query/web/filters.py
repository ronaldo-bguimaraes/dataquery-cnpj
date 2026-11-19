"""Validation and normalization of the listing query parameters."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterable, Mapping

from src.query.domain.codebook import MATRIZ_FILIAL, PORTES, SITUACOES
from src.query.domain.normalization import is_valid_cnpj, normalize_text
from src.query.domain.segmentos import is_segmento_valido

UFS = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG",
    "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
}

SIMPLE_VALUES = {"sim", "nao", "qualquer"}

IDADE_MAXIMA = 200
FILIAIS_MAXIMAS = 10_000


class FilterError(ValueError):
    """A validation error that can be shown to the user."""

    def __init__(self, field: str, message: str) -> None:
        super().__init__(message)
        self.field = field
        self.message = message


@dataclass(frozen=True)
class EstabelecimentoFiltros:
    q: str | None = None
    situacao: str | None = None
    uf: str | None = None
    municipio: str | None = None
    cnae: str | None = None
    porte: str | None = None
    matriz_filial: str | None = None
    abertura_de: date | None = None
    abertura_ate: date | None = None
    simples: str = "qualquer"
    mei: str = "qualquer"
    segmento: str | None = None
    idade_min: int | None = None
    idade_max: int | None = None
    filiais_min: int | None = None
    filiais_max: int | None = None
    com_contato: str = "qualquer"
    pagina: int = 1
    # Referência para idade (fim da competência). Não é um parâmetro de URL.
    referencia: date = field(default_factory=date.today)

    def as_query_params(self, *, include_page: bool = True) -> dict[str, str]:
        values: dict[str, str] = {}
        for name in (
            "q",
            "situacao",
            "uf",
            "municipio",
            "cnae",
            "porte",
            "matriz_filial",
            "segmento",
        ):
            value = getattr(self, name)
            # `situacao=""` é uma escolha explícita por "todas" e precisa
            # sobreviver à paginação; os demais filtros vazios são omitidos.
            if value or (name == "situacao" and value is not None):
                values[name] = str(value)
        if self.abertura_de:
            values["abertura_de"] = self.abertura_de.isoformat()
        if self.abertura_ate:
            values["abertura_ate"] = self.abertura_ate.isoformat()
        if self.simples != "qualquer":
            values["simples"] = self.simples
        if self.mei != "qualquer":
            values["mei"] = self.mei
        for name in ("idade_min", "idade_max", "filiais_min", "filiais_max"):
            value = getattr(self, name)
            if value is not None:
                values[name] = str(value)
        if self.com_contato != "qualquer":
            values["com_contato"] = self.com_contato
        if include_page and self.pagina > 1:
            values["pagina"] = str(self.pagina)
        return values


def _pairs(values: Mapping[str, Any] | Iterable[tuple[str, str]]) -> list[tuple[str, str]]:
    if hasattr(values, "multi_items"):
        return [(str(key), str(value)) for key, value in values.multi_items()]  # type: ignore[union-attr]
    if isinstance(values, Mapping):
        pairs: list[tuple[str, str]] = []
        for key, value in values.items():
            if isinstance(value, (list, tuple)):
                pairs.extend((str(key), str(item)) for item in value)
            elif value is not None:
                pairs.append((str(key), str(value)))
        return pairs
    return [(str(key), str(value)) for key, value in values]


def parse_filters(
    values: Mapping[str, Any] | Iterable[tuple[str, str]],
    *,
    referencia: date | None = None,
) -> EstabelecimentoFiltros:
    grouped: dict[str, list[str]] = defaultdict(list)
    for key, value in _pairs(values):
        grouped[key].append(value)

    single_valued = {
        "q", "situacao", "uf", "municipio", "cnae", "porte",
        "matriz_filial", "abertura_de", "abertura_ate", "simples", "mei", "pagina",
        "segmento", "idade_min", "idade_max", "filiais_min", "filiais_max",
        "com_contato",
    }
    for key, items in grouped.items():
        if len(items) > 1 and key in single_valued:
            raise FilterError(key, f"O filtro {key} deve ter um único valor.")

    def get(name: str) -> str | None:
        items = grouped.get(name)
        if not items:
            return None
        text = str(items[0]).strip()
        return text or None

    q = get("q")
    if q:
        digits = re.sub(r"\D", "", q)
        only_digits = q.isdigit()
        looks_masked_cnpj = bool(digits) and len(digits) == 14 and not only_digits
        if only_digits or looks_masked_cnpj:
            if len(digits) != 14 or not is_valid_cnpj(digits):
                raise FilterError("q", "CNPJ inválido.")
            q = digits
        else:
            q = normalize_text(q)
            if len(q) < 3:
                raise FilterError("q", "Informe ao menos 3 caracteres para buscar por nome.")

    situacao = None
    if "situacao" in grouped:
        raw_situacao = grouped["situacao"][0].strip()
        if raw_situacao:
            situacao = raw_situacao.zfill(2)
            if situacao not in SITUACOES:
                raise FilterError("situacao", "Situação cadastral inválida.")
        else:
            situacao = ""

    uf = get("uf")
    if uf:
        uf = uf.upper()
        if uf not in UFS:
            raise FilterError("uf", "UF inválida.")

    municipio = get("municipio")
    if municipio:
        digits = re.sub(r"\D", "", municipio)
        if len(digits) < 4 or len(digits) > 7 or digits != municipio:
            raise FilterError("municipio", "Município inválido.")
        municipio = digits

    cnae = get("cnae")
    if cnae:
        digits = re.sub(r"\D", "", cnae)
        if len(digits) != 7 or digits != cnae:
            raise FilterError("cnae", "CNAE deve ter 7 dígitos.")
        cnae = digits

    porte = get("porte")
    if porte:
        porte = porte.zfill(2)
        if porte not in PORTES:
            raise FilterError("porte", "Porte inválido.")

    matriz_filial = get("matriz_filial")
    if matriz_filial and matriz_filial not in MATRIZ_FILIAL:
        raise FilterError("matriz_filial", "Indicador de matriz/filial inválido.")

    def parse_iso_date(name: str) -> date | None:
        value = get(name)
        if not value:
            return None
        try:
            return date.fromisoformat(value)
        except ValueError:
            raise FilterError(name, "Data inválida; use AAAA-MM-DD.") from None

    abertura_de = parse_iso_date("abertura_de")
    abertura_ate = parse_iso_date("abertura_ate")
    if abertura_de and abertura_ate and abertura_de > abertura_ate:
        raise FilterError("abertura_de", "A data inicial não pode ser posterior à final.")

    simples = (get("simples") or "qualquer").lower()
    mei = (get("mei") or "qualquer").lower()
    if simples not in SIMPLE_VALUES:
        raise FilterError("simples", "Opção de Simples inválida.")
    if mei not in SIMPLE_VALUES:
        raise FilterError("mei", "Opção de MEI inválida.")

    pagina_text = get("pagina")
    if pagina_text is None:
        pagina = 1
    else:
        try:
            pagina = int(pagina_text)
        except ValueError:
            raise FilterError("pagina", "Página inválida.") from None
        if pagina < 1:
            raise FilterError("pagina", "Página deve ser maior ou igual a 1.")

    segmento = get("segmento")
    if segmento:
        segmento = segmento.upper()
        if not is_segmento_valido(segmento):
            raise FilterError("segmento", "Segmento inválido.")

    def parse_bounded_int(name: str, maximum: int, *, label: str) -> int | None:
        text = get(name)
        if text is None:
            return None
        try:
            number = int(text)
        except ValueError:
            raise FilterError(name, f"{label} deve ser um número inteiro.") from None
        if number < 0 or number > maximum:
            raise FilterError(name, f"{label} deve estar entre 0 e {maximum}.")
        return number

    idade_min = parse_bounded_int("idade_min", IDADE_MAXIMA, label="A idade mínima")
    idade_max = parse_bounded_int("idade_max", IDADE_MAXIMA, label="A idade máxima")
    if idade_min is not None and idade_max is not None and idade_min > idade_max:
        raise FilterError(
            "idade_min", "A idade mínima não pode ser maior que a máxima."
        )

    filiais_min = parse_bounded_int(
        "filiais_min", FILIAIS_MAXIMAS, label="O mínimo de filiais"
    )
    filiais_max = parse_bounded_int(
        "filiais_max", FILIAIS_MAXIMAS, label="O máximo de filiais"
    )
    if (
        filiais_min is not None
        and filiais_max is not None
        and filiais_min > filiais_max
    ):
        raise FilterError(
            "filiais_min", "O mínimo de filiais não pode ser maior que o máximo."
        )

    com_contato = (get("com_contato") or "qualquer").lower()
    if com_contato not in SIMPLE_VALUES:
        raise FilterError("com_contato", "Opção de contato inválida.")

    return EstabelecimentoFiltros(
        q=q,
        situacao=situacao,
        uf=uf,
        municipio=municipio,
        cnae=cnae,
        porte=porte,
        matriz_filial=matriz_filial,
        abertura_de=abertura_de,
        abertura_ate=abertura_ate,
        simples=simples,
        mei=mei,
        segmento=segmento,
        idade_min=idade_min,
        idade_max=idade_max,
        filiais_min=filiais_min,
        filiais_max=filiais_max,
        com_contato=com_contato,
        pagina=pagina,
        referencia=referencia or date.today(),
    )
