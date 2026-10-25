"""AI host identity, vendor terms, risk statement and request id."""

from moneybin.privacy.disclosure import (
    disclosure_request_id,
    host_for_recipient_name,
    host_from_client_info,
    host_from_environment,
    risk_statement,
    terms_version_for,
)


def test_known_client_info_is_identified() -> None:
    host = host_from_client_info("claude-ai", "1.2.0", transport="stdio")
    assert host.identified and host.recipient_key == "local:claude-ai"
    assert host.source == "reported_by_client" and host.terms is not None


def test_client_name_is_normalized() -> None:
    host = host_from_client_info("  Claude-AI ", "1", transport="stdio")
    assert host.recipient_key == "local:claude-ai"


def test_unknown_client_name_is_unidentified() -> None:
    host = host_from_client_info("some-new-app", "0.1", transport="stdio")
    assert host.identified is False and host.recipient_key is None
    assert host.name == "some-new-app"  # shown as reported


def test_missing_client_info_is_unidentified() -> None:
    assert host_from_client_info(None, None, transport="stdio").source == "none"


def test_recipient_key_ignores_version() -> None:
    a = host_from_client_info("claude-ai", "1.0", transport="stdio")
    b = host_from_client_info("claude-ai", "2.0", transport="stdio")
    assert a.recipient_key == b.recipient_key


def test_remote_transport_changes_key() -> None:
    assert (
        host_from_client_info("claude-ai", "1", transport="http").recipient_key
        != host_from_client_info("claude-ai", "1", transport="stdio").recipient_key
    )


def test_cli_environment_marker() -> None:
    host = host_from_environment({"CLAUDECODE": "1"})
    assert host.identified and host.source == "cli_environment"
    assert host.recipient_key == "local:claude-code"
    assert host_from_environment({}).identified is False


def test_multi_model_host_terms_say_depends_on_model() -> None:
    host = host_from_client_info("cursor-vscode", "1", transport="stdio")
    assert host.identified
    assert "depends on the model" in risk_statement(host, file_names=["a.pdf"])


def test_risk_statement_contents() -> None:
    text = risk_statement(
        host_from_client_info("claude-ai", "1", transport="stdio"),
        file_names=["statement.pdf"],
    )
    for needle in (
        "statement.pdf",
        "account numbers",
        "cannot pull it back",
        "If you already shared this file with your AI app directly",
        "moneybin privacy revoke",
    ):
        assert needle in text


def test_risk_statement_for_unidentified_host() -> None:
    text = risk_statement(
        host_from_client_info("some-new-app", "0.1", transport="stdio"),
        file_names=["a.pdf"],
    )
    assert "some-new-app" in text and "cannot pull it back" in text


def test_request_id_changes_with_inputs() -> None:
    host = host_from_environment({"CLAUDECODE": "1"})
    base = disclosure_request_id(file_sha256s=["aa", "bb"], host=host)
    assert base == disclosure_request_id(file_sha256s=["bb", "aa"], host=host)
    assert base != disclosure_request_id(file_sha256s=["aa"], host=host)
    assert base != disclosure_request_id(
        file_sha256s=["aa", "bb"], host=host_from_environment({})
    )


def test_terms_version_for_unidentified_is_none() -> None:
    assert terms_version_for(host_from_environment({})) == "none"


def test_risk_statement_with_no_files_has_no_empty_artifact() -> None:
    host = host_from_client_info("claude-ai", "1", transport="stdio")
    text = risk_statement(host, file_names=[])
    assert "To read ," not in text and "To read  " not in text
    assert "your PDF statements" in text


def test_unidentified_host_name_is_capped_and_printable() -> None:
    raw = "evil\x1b[31m\nname" + "x" * 500
    host = host_from_client_info(raw, "1", transport="stdio")
    text = risk_statement(host, file_names=["a.pdf"])
    assert "\x1b" not in text and "\n\n" not in text.split("(as reported")[0]
    assert "x" * 65 not in text


def test_host_for_recipient_name_known_and_unknown() -> None:
    host = host_for_recipient_name(" Claude-Code ")
    assert host is not None and host.identified
    assert host.recipient_key == "local:claude-code" and host.transport == "cli"
    assert host_for_recipient_name("nope") is None
