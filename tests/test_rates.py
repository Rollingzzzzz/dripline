"""Spec parsing tests for dripline.rates."""

from dripline.rates import parse_rate


def test_word_units():
    assert parse_rate("5/minute") == (5 / 60, 5)
    assert parse_rate("1000/second") == (1000.0, 1000)
    assert parse_rate("100/Hour") == (100 / 3600, 100)
    assert parse_rate("2/days") == (2 / 86400, 2)
    assert parse_rate("3/m") == (3 / 60, 3)


def test_window_seconds_form():
    assert parse_rate("1000/10s") == (100.0, 1000)
    assert parse_rate("50/0.2s") == (250.0, 50)


def test_bad_specs_raise():
    for bad in ("100", "x/minute", "0/minute", "-3/s", "5/lightyear", "5/0s", ""):
        try:
            parse_rate(bad)
        except ValueError:
            continue
        raise AssertionError(f"spec {bad!r} should have raised")


if __name__ == "__main__":
    test_word_units()
    test_window_seconds_form()
    test_bad_specs_raise()
    print("all rates tests passed")
