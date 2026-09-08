from ingestion.indicators import default_dim3, resolve_varcd


def test_resolve_varcd_known_name():
    assert resolve_varcd("median_price_per_m2") == "0012239"


def test_resolve_varcd_passes_through_raw_varcd():
    assert resolve_varcd("0099999") == "0099999"


def test_default_dim3_known_indicator():
    assert default_dim3("median_price_per_m2") == "T"


def test_default_dim3_indicator_without_dim3():
    assert default_dim3("number_of_sales") is None


def test_default_dim3_unknown_name():
    assert default_dim3("not_a_real_indicator") is None
