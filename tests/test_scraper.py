import pytest
from ta import scraper # Assuming 'ta' is in PYTHONPATH or tests is run from project root

# Tests for to_number function
def test_to_number_simple_float():
    assert scraper.to_number("10.5") == 10.5

def test_to_number_integer():
    assert scraper.to_number("100") == 100.0 # Floats are returned

def test_to_number_with_k_multiplier():
    assert scraper.to_number("2.5k") == 2500
    assert scraper.to_number("10K") == 10000 # Test uppercase

def test_to_number_with_m_multiplier():
    assert scraper.to_number("1.2m") == 1200000
    assert scraper.to_number("5M") == 5000000

def test_to_number_with_b_multiplier():
    assert scraper.to_number("3b") == 3000000000
    assert scraper.to_number("0.5B") == 500000000 # Test fractional B

def test_to_number_with_percentage():
    assert scraper.to_number("50%") == 50.0
    assert scraper.to_number("12.5%") == 12.5

def test_to_number_hyphen_returns_zero():
    assert scraper.to_number("-") == 0

def test_to_number_invalid_string_returns_zero():
    assert scraper.to_number("invalid") == 0
    assert scraper.to_number("1.2.3") == 0
    assert scraper.to_number("10mk") == 0 # Invalid multiplier combo

def test_to_number_empty_string_returns_zero():
    # Based on current implementation, empty string might cause error or be handled by ValueError
    # Let's assume it should be 0, might need adjustment in `to_number` if it errors
    assert scraper.to_number("") == 0

def test_to_number_string_with_spaces():
    assert scraper.to_number(" 10.5 ") == 10.5
    assert scraper.to_number(" 2.5k ") == 2500
    assert scraper.to_number(" 50% ") == 50.0

def test_to_number_non_string_input():
    assert scraper.to_number(None) == 0
    assert scraper.to_number(123) == 0 # Current implementation expects string

# TODO: Add more tests for other functions in scraper.py if time permits
# e.g., finviz_urls, and mock external calls for finviz, download_page_cloudflare etc.
# For now, focusing on the pure function `to_number`.

# Example of how to run pytest:
# Ensure pytest is installed: pip install pytest
# From the project root directory, run: pytest
# or: python -m pytest
