import pytest

from ingestion.idealista_parser import parse_listing_location


def _page(items, agency_href=None, ad_id="35366924"):
    lis = "".join(f'<li class="header-map-list">\n{i}\n</li>' for i in items)
    agency = (
        f'<a id="agencyExternalLink" href="{agency_href}">{agency_href}</a>'
        if agency_href else ""
    )
    return (
        f'<html><body><input type="hidden" name="adId" value="{ad_id}">'
        f'<div id="headerMap"><h2>Location</h2><ul>{lis}</ul></div>{agency}</body></html>'
    )


def test_full_location_en_page():
    html = _page(
        ["Rua Aliança Operária Nn", "District Alto de Alcântara", "Alcântara", "Lisbon"],
        agency_href="https://www.kwportugal.pt/imovel/Venda/Apartamento/Lisboa/Lisboa/Ajuda/85324",
    )
    loc = parse_listing_location(html)
    assert loc.ad_id == "35366924"
    assert loc.street == "Rua Aliança Operária Nn"
    assert loc.neighbourhood == "Alto de Alcântara"
    assert loc.parish == "Alcântara"
    assert loc.municipality == "Lisboa"
    assert loc.agency_parish == "Ajuda"
    assert loc.parish_mismatch


def test_pt_page_without_street():
    loc = parse_listing_location(_page(["Bairro Alto de Alcântara", "Alcântara", "Lisboa"]))
    assert loc.street is None
    assert loc.neighbourhood == "Alto de Alcântara"
    assert loc.parish == "Alcântara"
    assert loc.municipality == "Lisboa"


def test_only_parish_and_municipality():
    loc = parse_listing_location(_page(["Cedofeita", "Oporto"]))
    assert loc.street is None and loc.neighbourhood is None
    assert loc.parish == "Cedofeita"
    assert loc.municipality == "Porto"


def test_agency_parish_match_ignores_accents_and_case():
    html = _page(
        ["Alcântara", "Lisbon"],
        agency_href="https://www.kwportugal.pt/imovel/Venda/Apartamento/Lisboa/Lisboa/alcantara/1",
    )
    assert not parse_listing_location(html).parish_mismatch


def test_unknown_agency_has_no_parish():
    html = _page(["Alcântara", "Lisbon"], agency_href="https://example-agency.pt/listing/1")
    loc = parse_listing_location(html)
    assert loc.agency_url == "https://example-agency.pt/listing/1"
    assert loc.agency_parish is None
    assert not loc.parish_mismatch


def test_missing_header_map_raises():
    with pytest.raises(ValueError, match="headerMap"):
        parse_listing_location("<html><body>nothing here</body></html>")


def test_too_few_items_raises():
    with pytest.raises(ValueError, match="at least parish and municipality"):
        parse_listing_location(_page(["Lisbon"]))
