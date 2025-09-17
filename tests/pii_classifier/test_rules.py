import pytest

from pii_classifier.rules_loader import Rule, RuleEvaluation, load_rules


@pytest.fixture(scope="module")
def rules_by_name() -> dict[str, Rule]:
    rules = load_rules("sample/regex/rules.yml")
    return {rule.name: rule for rule in rules}


def test_email_rule_positive(rules_by_name: dict[str, Rule]) -> None:
    rule = rules_by_name["pii_email"]
    samples = [
        "alice@example.com",
        "bob.smith+orders@example.co.in",
        "charlie@work.org",
        "dana.white@company.io",
        "eve@test.net",
    ]
    evaluation: RuleEvaluation = rule.evaluate("email", samples)
    assert evaluation.is_match
    assert evaluation.confidence >= 0.6


def test_email_rule_negative(rules_by_name: dict[str, Rule]) -> None:
    rule = rules_by_name["pii_email"]
    samples = ["CAPTURED", "FAILED", "PENDING", "SUCCEEDED", "SETTLED"]
    evaluation = rule.evaluate("status", samples)
    assert not evaluation.is_match


def test_pan_regex(rules_by_name: dict[str, Rule]) -> None:
    rule = rules_by_name["pii_pan"]
    samples = ["AAAAA9999A", "BBBBB1234C", "CCCCC9876D", "EEEEE0000F", "GGGGG1234H"]
    evaluation = rule.evaluate("pan", samples)
    assert evaluation.is_match
    assert evaluation.value_ratio >= 0.5


def test_aadhaar_requires_digit_length(rules_by_name: dict[str, Rule]) -> None:
    rule = rules_by_name["pii_aadhaar"]
    samples = ["123456789012", "111122223333", "999900001111", "123123123123", "987654321098"]
    evaluation = rule.evaluate("aadhaar", samples)
    assert evaluation.is_match
    assert evaluation.value_ratio == 1


def test_name_rule_name_only(rules_by_name: dict[str, Rule]) -> None:
    rule = rules_by_name["pii_name"]
    samples = ["Alice Johnson", "Bob", "Charlie Brown", "Dana White", "Eric Cartman"]
    evaluation = rule.evaluate("full_name", samples)
    assert evaluation.is_match
    assert evaluation.name_match
