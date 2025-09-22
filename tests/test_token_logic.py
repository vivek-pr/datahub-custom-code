from action import token_logic


def test_generate_token_is_deterministic():
    value = "user@example.com"
    token1 = token_logic.generate_token(value)
    token2 = token_logic.generate_token(value)
    assert token1 == token2
    assert token_logic.is_token(token1)


def test_tokenize_if_needed_skips_existing_tokens():
    original = "555-1234"
    token = token_logic.generate_token(original)
    assert token_logic.tokenize_if_needed(token) == token
    assert token_logic.tokenize_if_needed(original) == token
    assert token_logic.tokenize_if_needed(None) is None
