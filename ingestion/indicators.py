"""Registry of known INE (Instituto Nacional de Estatistica) indicator codes.

Each entry maps a friendly name to the INE `varcd` used by the
`pindica.jsp` JSON API (see ine_client.py). `verified` means the varcd
has been confirmed against a live call to that API; unverified entries
are best-effort guesses that should be checked before relying on them.

To find a new varcd: ine.pt -> Bases de Dados -> search for the series
-> "Alterar condicoes de selecao" -> switch "Arvore" to "Codigos".
"""

from dataclasses import dataclass
from enum import Enum


class Frequency(str, Enum):
    QUARTERLY = "quarterly"
    MONTHLY = "monthly"
    ANNUAL = "annual"


@dataclass(frozen=True)
class IndicatorInfo:
    varcd: str
    description: str
    frequency: Frequency
    verified: bool
    notes: str = ""
    default_dim3: str | None = None


KNOWN_INDICATORS: dict[str, IndicatorInfo] = {
    "median_price_per_m2": IndicatorInfo(
        varcd="0012239",
        description=(
            "Valor mediano das vendas de alojamentos familiares "
            "(Metodologia 2022 - EUR/m2) por NUTS e domicilio fiscal "
            "do comprador"
        ),
        frequency=Frequency.QUARTERLY,
        verified=True,
        notes=(
            "dim_3 = domicilio fiscal do comprador: '1' = Territorio "
            "nacional, '2' = Estrangeiro, '21' = Uniao Europeia, "
            "'22' = Restantes paises, 'T' = Total (all buyers). "
            "Defaults to dim3='T'."
        ),
        default_dim3="T",
    ),
    "number_of_sales": IndicatorInfo(
        varcd="0014363",
        description=(
            "Vendas de alojamentos familiares nos ultimos 12 meses "
            "(Metodologia 2022 - N.) por NUTS"
        ),
        frequency=Frequency.QUARTERLY,
        verified=True,
        notes="No dim_3 dimension for this indicator.",
    ),
    "housing_price_index": IndicatorInfo(
        varcd="0014765",
        description=(
            "Indice de precos da habitacao / IPHab (Base - 2025) por "
            "categoria do alojamento familiar"
        ),
        frequency=Frequency.QUARTERLY,
        verified=True,
        notes=(
            "dim_3 = categoria do alojamento: 'H1' = Total, "
            "'H11' = Novos, 'H12' = Existentes. Defaults to dim3='H1'."
        ),
        default_dim3="H1",
    ),
    "consumer_price_index": IndicatorInfo(
        varcd="0014640",
        description=(
            "Indice de precos no consumidor / IPC (Base - 2025) por "
            "localizacao geografica e agregados especiais"
        ),
        frequency=Frequency.MONTHLY,
        verified=True,
        notes=(
            "dim_3 = agregado especial: 'T' = Total (headline IPC), "
            "'001' = Total exceto habitacao (useful to avoid "
            "circularity when deflating housing prices), plus other "
            "component breakdowns. Level index, not a rate of change. "
            "Defaults to dim3='T'."
        ),
        default_dim3="T",
    ),
}


def default_dim3(name_or_varcd: str) -> str | None:
    """The registry's default dim_3 filter for a known indicator name, else None."""
    info = KNOWN_INDICATORS.get(name_or_varcd)
    return info.default_dim3 if info else None


def resolve_varcd(name_or_varcd: str) -> str:
    """Resolve a KNOWN_INDICATORS name to its varcd, or pass through a raw varcd."""
    if name_or_varcd in KNOWN_INDICATORS:
        return KNOWN_INDICATORS[name_or_varcd].varcd
    return name_or_varcd
