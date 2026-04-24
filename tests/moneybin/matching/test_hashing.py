"""Tests for gold key hashing."""

from moneybin.matching.hashing import gold_key_matched, gold_key_unmatched


class TestGoldKeyUnmatched:
    """Tests for gold_key_unmatched."""

    def test_deterministic(self) -> None:
        key1 = gold_key_unmatched("csv", "abc123", "acct1")
        key2 = gold_key_unmatched("csv", "abc123", "acct1")
        assert key1 == key2

    def test_length_is_16_hex(self) -> None:
        key = gold_key_unmatched("ofx", "FITID001", "checking")
        assert len(key) == 16
        assert all(c in "0123456789abcdef" for c in key)

    def test_different_inputs_produce_different_keys(self) -> None:
        key1 = gold_key_unmatched("csv", "abc", "acct1")
        key2 = gold_key_unmatched("ofx", "abc", "acct1")
        assert key1 != key2

    def test_pipe_delimited_input(self) -> None:
        import hashlib

        expected = hashlib.sha256(b"csv|txn123|acct1").hexdigest()[:16]
        assert gold_key_unmatched("csv", "txn123", "acct1") == expected


class TestGoldKeyMatched:
    """Tests for gold_key_matched."""

    def test_deterministic(self) -> None:
        tuples = [("csv", "abc", "acct1"), ("ofx", "xyz", "acct1")]
        key1 = gold_key_matched(tuples)
        key2 = gold_key_matched(tuples)
        assert key1 == key2

    def test_order_independent(self) -> None:
        key1 = gold_key_matched([
            ("ofx", "xyz", "acct1"),
            ("csv", "abc", "acct1"),
        ])
        key2 = gold_key_matched([
            ("csv", "abc", "acct1"),
            ("ofx", "xyz", "acct1"),
        ])
        assert key1 == key2

    def test_length_is_16_hex(self) -> None:
        key = gold_key_matched([("csv", "a", "x"), ("ofx", "b", "x")])
        assert len(key) == 16

    def test_different_from_unmatched(self) -> None:
        matched = gold_key_matched([("csv", "abc", "acct1"), ("ofx", "xyz", "acct1")])
        unmatched_csv = gold_key_unmatched("csv", "abc", "acct1")
        unmatched_ofx = gold_key_unmatched("ofx", "xyz", "acct1")
        assert matched != unmatched_csv
        assert matched != unmatched_ofx
