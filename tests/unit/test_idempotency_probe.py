import pytest

from tools.verify_poc import is_tokenized_value, summarize_tokenization


def test_is_tokenized_value_matches_expected_pattern():
    assert is_tokenized_value("tok_abcd1234ef_poc")
    assert not is_tokenized_value("tok_short_poc")
    assert not is_tokenized_value(123)
    assert not is_tokenized_value(None)


def test_summarize_tokenization_counts_updates():
    before = ["alice", "bob", "carol"]
    after = ["tok_abcdef1234_poc", "bob", "tok_fedcba4321_poc"]
    summary = summarize_tokenization(before, after)
    assert summary["updated"] == 2
    assert summary["unchanged"] == 1
    assert summary["tokenized"] == 2


def test_summarize_tokenization_requires_equal_lengths():
    with pytest.raises(ValueError):
        summarize_tokenization(["a"], ["a", "b"])
