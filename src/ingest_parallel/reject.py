"""Ledger de rejeições de ingestão (FR-038, data-model.md §5.4)."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from src.ingest_parallel.observability import sanitize_text

REJECT_TYPES = {
    "schema_mismatch",
    "duplicate",
    "invalid_data",
    "missing_required_field",
    "max_length_exceeded",
}


class InvalidRejectType(ValueError):
    """Tentativa de registrar um tipo de rejeição fora do contrato."""


@dataclass(frozen=True)
class RejectRecord:
    """Uma rejeição contabilizada, sempre sem PII."""

    load_id: str
    partition_id: str
    table: str
    reject_type: str
    reason_detail: str
    source_file_hash: str = ""
    source_row_offset: int | None = None
    rejected_at: str = ""

    def __post_init__(self) -> None:
        if self.reject_type not in REJECT_TYPES:
            raise InvalidRejectType(
                f"reject_type inválido: {self.reject_type}; permitidos: {sorted(REJECT_TYPES)}"
            )
        object.__setattr__(self, "reason_detail", sanitize_text(self.reason_detail))
        if not self.rejected_at:
            object.__setattr__(self, "rejected_at", time.strftime("%Y-%m-%dT%H:%M:%S%z"))


@dataclass
class RejectLedger:
    """Coordena registros de rejeição por execução, com contagem agregada."""

    load_id: str
    records: list[RejectRecord] = field(default_factory=list)

    def add(
        self,
        partition_id: str,
        table: str,
        reject_type: str,
        reason_detail: str,
        *,
        source_file_hash: str = "",
        source_row_offset: int | None = None,
    ) -> RejectRecord:
        record = RejectRecord(
            load_id=self.load_id,
            partition_id=partition_id,
            table=table,
            reject_type=reject_type,
            reason_detail=reason_detail,
            source_file_hash=source_file_hash,
            source_row_offset=source_row_offset,
        )
        self.records.append(record)
        return record

    def counts_by_type(self) -> dict[str, int]:
        counts: dict[str, int] = {reject_type: 0 for reject_type in sorted(REJECT_TYPES)}
        for record in self.records:
            counts[record.reject_type] += 1
        return counts

    def count(self) -> int:
        return len(self.records)

    def summary(self) -> dict[str, int]:
        """Resumo auditável — sem PII por construção (RejectRecord sanitiza)."""

        payload = {"load_id": self.load_id, "total": self.count()}
        payload.update(self.counts_by_type())
        return payload

    def ensure_pii_free(self) -> None:
        """Garantia de auditoria (SC-009): nenhuma rejeição guarda CPF completo."""

        from src.query.domain.masking import has_full_cpf

        for record in self.records:
            if has_full_cpf(record.reason_detail):
                raise AssertionError("rejeição com CPF não-mascarado")
