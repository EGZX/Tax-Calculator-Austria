from decimal import Decimal

import pytest

from tax_calc_at.normalize import (
    country_from_isin,
    is_valid_isin,
    normalize_currency,
    parse_date,
    parse_decimal,
)


@pytest.mark.parametrize(
    "raw,sep,expected",
    [
        ("1.234,56", ",", Decimal("1234.56")),
        ("1234,56", ",", Decimal("1234.56")),
        ("0,00", ",", Decimal("0")),
        ("", ",", Decimal("0")),
        ("1,234.56", ".", Decimal("1234.56")),
        ("1234.56", ".", Decimal("1234.56")),
        ("-110.19", ".", Decimal("-110.19")),
        ("0.5081810000", ".", Decimal("0.5081810000")),
        ("1.676,64", ",", Decimal("1676.64")),
    ],
)
def test_parse_decimal(raw, sep, expected):
    assert parse_decimal(raw, decimal_sep=sep) == expected


def test_parse_date_iso():
    assert parse_date("2024-12-30").year == 2024


def test_parse_date_with_time():
    assert parse_date("2024-12-30 17:20:29").day == 30


def test_isin_valid():
    assert is_valid_isin("US88160R1014")  # Tesla
    assert is_valid_isin("DE000A1EWWW0")  # Adidas
    assert is_valid_isin("IE00BLRPRL42")
    assert not is_valid_isin("US88160R1015")  # bad checksum
    assert not is_valid_isin("")
    assert not is_valid_isin("FOO")


def test_country_from_isin():
    assert country_from_isin("US88160R1014") == "US"
    assert country_from_isin("DE000A1EWWW0") == "DE"
    assert country_from_isin(None) is None


def test_currency_normalize():
    assert normalize_currency("eur") == "EUR"
    assert normalize_currency(None) == "EUR"
    with pytest.raises(ValueError):
        normalize_currency("EU")
