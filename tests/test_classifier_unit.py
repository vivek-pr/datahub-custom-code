"""Unit tests for the regex-based PII classifier rules."""

from actions.base64_action.configuration import DatabaseConfig
from actions.pii_classifier import ClassifierConfig, RegexPIIClassifier


def _load_rules_by_name():
    config = ClassifierConfig.load()
    classifier = RegexPIIClassifier(config, DatabaseConfig())
    return {rule.name: rule for rule in classifier.rules}


def test_email_patterns_match_names_and_values():
    rules = _load_rules_by_name()
    email = rules["email"]
    assert email.column_regex is not None
    assert email.column_regex.search("user_email")
    assert email.column_regex.search("primary_mail")
    assert email.value_regex is not None
    assert email.value_regex.search("alice@example.com")


def test_phone_pattern_covers_common_formats():
    rules = _load_rules_by_name()
    phone = rules["phone"]
    assert phone.column_regex is not None
    assert phone.column_regex.search("phone_number")
    assert phone.column_regex.search("mobile")
    assert phone.value_regex is not None
    assert phone.value_regex.search("+1 (415) 555-1212")


def test_id_pattern_allows_hex_and_dash_values():
    rules = _load_rules_by_name()
    generic = rules["id_generic"]
    assert generic.column_regex is not None
    assert generic.column_regex.search("user_id")
    assert generic.column_regex.search("external_identifier")
    assert generic.value_regex is not None
    assert generic.value_regex.search("ABCDEF12-3456")


def test_address_pattern_is_name_only():
    rules = _load_rules_by_name()
    address = rules["address"]
    assert address.column_regex is not None
    assert address.column_regex.search("billing_address")
    assert address.value_regex is None
