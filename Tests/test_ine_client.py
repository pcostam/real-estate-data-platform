import pytest

from ingestion.ine_client import parse_period


@pytest.mark.parametrize(
    "period, expected",
    [
        ("1.º Trimestre de 2023", (2023, 1)),
        ("4.º Trimestre de 2025", (2025, 4)),
        ("Janeiro de 2024", (2024, 1)),
        ("Março de 2024", (2024, 3)),
        ("Dezembro de 2024", (2024, 12)),
        ("2023", (2023, None)),
    ],
)
def test_parse_period_valid(period, expected):
    assert parse_period(period) == expected


def test_parse_period_unrecognized_format_raises():
    with pytest.raises(ValueError):
        parse_period("not a period")
