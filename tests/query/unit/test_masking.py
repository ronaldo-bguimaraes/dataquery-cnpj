from __future__ import annotations

from src.query.domain.masking import CPF_MASK, mask_document, redact_cpf_patterns


def test_mask_document_hides_full_cpf():
    masked = mask_document("12345678901")
    assert masked == CPF_MASK
    assert "12345678901" not in masked


def test_mask_document_normalizes_already_masked_value():
    assert mask_document("***240659**") == CPF_MASK


def test_mask_document_preserves_empty():
    assert mask_document("") == ""
    assert mask_document(None) == ""


def test_redact_cpf_patterns_in_free_text():
    text = "Contato: 123.456.789-00 / 12345678901 / CNPJ 11.222.333/0001-81"
    redacted = redact_cpf_patterns(text)
    assert "123.456.789-00" not in redacted
    assert "12345678901" not in redacted
    assert "11.222.333/0001-81" in redacted
