"""Registry of known INE (Instituto Nacional de Estatistica) indicator codes.

Each entry maps a friendly name to the INE `varcd` used by the
`pindica.jsp` JSON API (see ine_client.py). `verified` means the varcd
has been confirmed against a live call to that API; unverified entries
are best-effort guesses that should be checked before relying on them.

To find a new varcd: ine.pt -> Bases de Dados -> search for the series
-> "Alterar condicoes de selecao" -> switch "Arvore" to "Codigos".
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class IndicatorInfo:
    varcd: str
    description: str
    frequency: str  # "quarterly" | "monthly" | "annual"
    verified: bool
    notes: str = ""


KNOWN_INDICATORS: dict[str, IndicatorInfo] = {
    "median_price_per_m2": IndicatorInfo(
        varcd="0012239",
        description=(
            "Valor mediano das vendas de alojamentos familiares "
            "(Metodologia 2022 - EUR/m2) por NUTS e domicilio fiscal "
            "do comprador"
        ),
        frequency="quarterly",
        verified=True,
        notes="dim_3 = domicilio fiscal do comprador (currently always '1' / Territorio nacional).",
    ),
    "number_of_sales": IndicatorInfo(
        varcd="0014363",
        description=(
            "Vendas de alojamentos familiares nos ultimos 12 meses "
            "(Metodologia 2022 - N.) por NUTS"
        ),
        frequency="quarterly",
        verified=True,
    ),
    "housing_price_index": IndicatorInfo(
        varcd="0014765",
        description=(
            "Indice de precos da habitacao / IPHab (Base - 2025) por "
            "categoria do alojamento familiar"
        ),
        frequency="quarterly",
        verified=True,
        notes=(
            "dim_3 = categoria do alojamento: 'H1' = Total, "
            "'H11' = Novos, 'H12' = Existentes. Filter to dim3='H1' "
            "for the headline index."
        ),
    ),
    "consumer_price_index": IndicatorInfo(
        varcd="0014640",
        description=(
            "Indice de precos no consumidor / IPC (Base - 2025) por "
            "localizacao geografica e agregados especiais"
        ),
        frequency="monthly",
        verified=True,
        notes=(
            "dim_3 = agregado especial (e.g. '001' = Total exceto "
            "habitacao). Level index suitable for deflating nominal "
            "prices, not a rate of change."
        ),
    ),
}


def resolve_varcd(name_or_varcd: str) -> str:
    """Resolve a KNOWN_INDICATORS name to its varcd, or pass through a raw varcd."""
    if name_or_varcd in KNOWN_INDICATORS:
        return KNOWN_INDICATORS[name_or_varcd].varcd
    return name_or_varcd
