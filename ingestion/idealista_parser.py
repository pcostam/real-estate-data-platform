"""Extract location fields from a saved idealista.pt listing page.

The listing's "Location" box (`#headerMap`) is a list ordered from most
to least specific: street, idealista neighbourhood ("District X" on /en/
pages, "Bairro X" on /pt/), parish, municipality. Street and
neighbourhood are optional; parish and municipality are always the last
two items.

idealista's parish is its own zoning and can disagree with the official
one near parish boundaries. When the agency's own link encodes a parish
(currently only kwportugal.pt), it is returned as `agency_parish` so
mismatches can be flagged.

Saved pages carry no coordinates: latitude/longitude are filled in by
JavaScript after load.
"""

import re
import unicodedata
from dataclasses import dataclass
from urllib.parse import urlparse

from bs4 import BeautifulSoup

_NEIGHBOURHOOD_PREFIXES = ("District ", "Bairro ")

# idealista's /en/ pages translate a few municipality names; INE uses Portuguese.
_MUNICIPALITY_PT = {
    "Lisbon": "Lisboa",
    "Oporto": "Porto",
}


@dataclass(frozen=True)
class ListingLocation:
    ad_id: str | None
    street: str | None
    neighbourhood: str | None
    parish: str | None
    municipality: str
    agency_url: str | None = None
    agency_parish: str | None = None

    @property
    def parish_mismatch(self) -> bool:
        """True when the agency's link names a different parish than idealista."""
        if self.parish is None or self.agency_parish is None:
            return False
        return _fold(self.parish) != _fold(self.agency_parish)


def _fold(name: str) -> str:
    """Case- and accent-insensitive key, so 'Alcantara' == 'Alcântara'."""
    decomposed = unicodedata.normalize("NFKD", name)
    return "".join(c for c in decomposed if not unicodedata.combining(c)).casefold().strip()


def _agency_parish(url: str) -> str | None:
    """Parish from a kwportugal.pt listing URL, else None.

    KW URLs look like /imovel/{deal}/{type}/{district}/{municipality}/{parish}/{id}.
    """
    parsed = urlparse(url)
    if not parsed.netloc.endswith("kwportugal.pt"):
        return None
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) == 7 and parts[0] == "imovel":
        return parts[5]
    return None


def parse_listing_location(html: str) -> ListingLocation:
    soup = BeautifulSoup(html, "html.parser")

    header_map = soup.select_one("#headerMap")
    if header_map is None:
        raise ValueError("No #headerMap element found; not an idealista listing page or its layout changed")
    items = [li.get_text(" ", strip=True) for li in header_map.select("li.header-map-list")]
    items = [i for i in items if i]
    if len(items) < 2:
        raise ValueError(f"Expected at least parish and municipality in #headerMap, got {items!r}")

    *head, parish, municipality = items
    neighbourhood = None
    street = None
    for item in head:
        prefix = next((p for p in _NEIGHBOURHOOD_PREFIXES if item.startswith(p)), None)
        if prefix is not None:
            neighbourhood = item[len(prefix):]
        elif street is None:
            street = item

    ad_id_input = soup.find("input", attrs={"name": "adId"})
    ad_id = ad_id_input.get("value") if ad_id_input else None
    if ad_id is None:
        match = re.search(r"adId=(\d+)", html)
        ad_id = match.group(1) if match else None

    agency_link = soup.select_one("a#agencyExternalLink")
    agency_url = agency_link.get("href") if agency_link else None

    return ListingLocation(
        ad_id=ad_id,
        street=street,
        neighbourhood=neighbourhood,
        parish=parish,
        municipality=_MUNICIPALITY_PT.get(municipality, municipality),
        agency_url=agency_url,
        agency_parish=_agency_parish(agency_url) if agency_url else None,
    )
