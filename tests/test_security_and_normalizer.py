"""Tests for security controls (CWE-1236, Zip-Bomb guard) and canonical normalizers."""

import zipfile
from pathlib import Path

import pytest

from brazil_data_gateway.core.normalizer import (
    CanonicalNormalizer,
    CheckDigitValidator,
    MonetaryNormalizer,
)
from brazil_data_gateway.core.security import (
    safe_extract_zip,
    sanitize_csv_value,
    verify_magic_bytes,
)


def test_magic_bytes_verification() -> None:
    """Test magic bytes validation for zip, parquet, sqlite, and gzip formats."""
    assert verify_magic_bytes(b"PK\x03\x04\x14\x00", "zip")
    assert verify_magic_bytes(b"PAR1\x00\x00\x00", "parquet")
    assert verify_magic_bytes(b"SQLite format 3\x00data", "sqlite")
    assert verify_magic_bytes(b"\x1f\x8b\x08\x00", "gzip")

    assert not verify_magic_bytes(b"INVALID_HEADER", "zip")
    assert not verify_magic_bytes(b"PAR2", "parquet")
    assert not verify_magic_bytes(b"SQLite format 2", "sqlite")

    with pytest.raises(ValueError, match="Unsupported format"):
        verify_magic_bytes(b"DATA", "unsupported_ext")


def test_zip_slip_rejection(tmp_path: Path) -> None:
    """Test safe_extract_zip rejects archives attempting directory traversal (CWE-22)."""
    zip_file = tmp_path / "slip_attack.zip"
    dest_dir = tmp_path / "extracted"

    # Craft archive with path traversal member
    with zipfile.ZipFile(zip_file, "w") as zf:
        zf.writestr("../../escape.txt", "malicious payload")

    with pytest.raises(ValueError, match="Zip slip detected"):
        safe_extract_zip(zip_file, dest_dir)


def test_zip_bomb_ratio_rejection(tmp_path: Path) -> None:
    """Test safe_extract_zip rejects high compression ratio archives (CWE-409)."""
    zip_file = tmp_path / "bomb.zip"
    dest_dir = tmp_path / "extracted_bomb"

    # 200 KB of repetitive zeros compresses to under 200 bytes (ratio > 1000)
    data = b"0" * 200_000
    with zipfile.ZipFile(zip_file, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("huge_zeroes.txt", data)

    with pytest.raises(ValueError, match="Zip bomb detected: compression ratio"):
        safe_extract_zip(zip_file, dest_dir, max_ratio=20.0)


def test_csv_formula_injection_sanitization() -> None:
    """Test CWE-1236 formula injection neutralization while preserving legitimate negative numbers."""
    # Dangerous formula triggers
    assert sanitize_csv_value("=cmd|'/C calc'!A0") == "'=cmd|'/C calc'!A0"
    assert sanitize_csv_value("+cmd|'/C calc'!A0") == "'+cmd|'/C calc'!A0"
    assert sanitize_csv_value("-cmd|'/C calc'!A0") == "'-cmd|'/C calc'!A0"
    assert sanitize_csv_value("@SUM(A1:A10)") == "'@SUM(A1:A10)"
    assert sanitize_csv_value("\tmalicious_tab") == "'\tmalicious_tab"
    assert sanitize_csv_value("\rline_break") == "'\rline_break"

    # Safe legitimate numbers MUST NOT be prefixed
    assert sanitize_csv_value("-12.34") == "-12.34"
    assert sanitize_csv_value("-5") == "-5"
    assert sanitize_csv_value("-0.001") == "-0.001"
    assert sanitize_csv_value("-123,45") == "-123,45"
    assert sanitize_csv_value("123.45") == "123.45"

    # Non-string types pass through unmodified
    assert sanitize_csv_value(123) == 123
    assert sanitize_csv_value(-45.6) == -45.6
    assert sanitize_csv_value(None) is None


def test_mod11_cnpj_and_cpf_validation() -> None:
    """Test deterministic Mod-11 validation for Brazilian CNPJ and CPF."""
    # Petrobras official CNPJ
    assert CheckDigitValidator.is_valid_cnpj("33.000.167/0001-01")
    assert CheckDigitValidator.is_valid_cnpj("33000167000101")
    # Repetitive and invalid check digits
    assert not CheckDigitValidator.is_valid_cnpj("11.111.111/1111-11")
    assert not CheckDigitValidator.is_valid_cnpj("33.000.167/0001-99")
    assert not CheckDigitValidator.is_valid_cnpj(None)
    assert not CheckDigitValidator.is_valid_cnpj("short")

    # Valid CPF test (computed Mod-11)
    valid_cpf = "123.456.789-09"
    assert CheckDigitValidator.is_valid_cpf(valid_cpf)
    assert CheckDigitValidator.is_valid_cpf("12345678909")
    assert not CheckDigitValidator.is_valid_cpf("000.000.000-00")
    assert not CheckDigitValidator.is_valid_cpf("123.456.789-99")
    assert not CheckDigitValidator.is_valid_cpf(None)


def test_ibge_7_validation() -> None:
    """Test 7-digit IBGE municipality Luhn check-digit validator."""
    # Boa Esperança do Norte (MT)
    assert CheckDigitValidator.is_valid_ibge_7("5101837")
    # Acrelândia (AC)
    assert CheckDigitValidator.is_valid_ibge_7("1200013")
    # São Paulo (SP)
    assert CheckDigitValidator.is_valid_ibge_7("3550308")
    # Invalid check digit
    assert not CheckDigitValidator.is_valid_ibge_7("5101839")
    assert not CheckDigitValidator.is_valid_ibge_7("123")
    assert not CheckDigitValidator.is_valid_ibge_7(None)


def test_canonical_normalizer_ibge_resolution() -> None:
    """Test municipality resolution: 6D legacy -> 7D, fuzzy unaccented name + UF inference."""
    normalizer = CanonicalNormalizer()

    # 6-digit legacy code to 7-digit official
    assert normalizer.to_ibge_7("510183") == "5101837"

    # Boa Esperança do Norte with and without UF
    assert normalizer.to_ibge_7("Boa Esperança do Norte", uf="MT") == "5101837"
    assert normalizer.to_ibge_7("boa esperanca do norte") == "5101837"

    rec = normalizer.get_municipio_record("Boa Esperança do Norte")
    assert rec is not None
    assert rec["id_municipio"] == "5101837"
    assert rec["sigla_uf"] == "MT"
    assert rec["nome_uf"] == "Mato Grosso"

    # Disambiguation with UF
    sp_rec = normalizer.get_municipio_record("São Paulo", uf="SP")
    assert sp_rec is not None
    assert sp_rec["id_municipio"] == "3550308"


def test_preserve_cpf_mask() -> None:
    """Test strict masking of CPF to ***.123.456-** format."""
    # Raw 11 digits
    assert CanonicalNormalizer.preserve_cpf_mask("12345678901") == "***.456.789-**"
    # Formatted
    assert CanonicalNormalizer.preserve_cpf_mask("123.456.789-01") == "***.456.789-**"
    # Text containing CPF
    text = "Assinado digitalmente pelo servidor CPF 123.456.789-01 em 2026."
    masked = CanonicalNormalizer.preserve_cpf_mask(text)
    assert "***.456.789-**" in masked
    assert "123.456.789-01" not in masked


def test_decode_gov_bytes() -> None:
    """Test decoding UTF-8 and CP1252/Latin-1 binaries with CRLF normalization."""
    utf8_raw = "Brasília, DF\r\nLinha 2".encode()
    assert CanonicalNormalizer.decode_gov_bytes(utf8_raw) == "Brasília, DF\nLinha 2"

    cp1252_raw = "Goiânia, GO\r\nLinha 2".encode("cp1252")
    assert CanonicalNormalizer.decode_gov_bytes(cp1252_raw) == "Goiânia, GO\nLinha 2"


def test_ptbr_decimal_parser() -> None:
    """Test parsing PT-BR currency and numeric strings and sentinel conversion."""
    assert CanonicalNormalizer.parse_ptbr_decimal("1.234.567,89") == 1234567.89
    assert CanonicalNormalizer.parse_ptbr_decimal("45,50") == 45.50
    assert CanonicalNormalizer.parse_ptbr_decimal(100.5) == 100.5

    # Sentinel values must map to None
    assert CanonicalNormalizer.parse_ptbr_decimal("SEM INFORMACAO") is None
    assert CanonicalNormalizer.parse_ptbr_decimal("-") is None
    assert CanonicalNormalizer.parse_ptbr_decimal("999") is None
    assert CanonicalNormalizer.parse_ptbr_decimal("null") is None
    assert CanonicalNormalizer.parse_ptbr_decimal("") is None


def test_monetary_normalizer() -> None:
    """Test IPCA deflation and foreign exchange conversion."""
    index_map = {"2023-01": 100.0, "2024-01": 105.0}
    deflated = MonetaryNormalizer.deflate_series(100.0, "2023-01", "2024-01", index_map)
    assert deflated == 105.0

    converted = MonetaryNormalizer.convert_fx(100.0, 5.25)
    assert converted == 525.0

    with pytest.raises(ValueError, match="Base period"):
        MonetaryNormalizer.deflate_series(100.0, "2020-01", "2024-01", index_map)
