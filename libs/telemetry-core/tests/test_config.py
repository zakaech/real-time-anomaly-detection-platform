"""Configuration must fail at startup, and must not leak secrets.

Every test chdirs into a temporary directory so a developer's ``.env`` at the
repository root cannot make an assertion pass or fail by accident.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from telemetry_core.config import (
    KafkaSettings,
    KafkaTopics,
    LoggingSettings,
    PostgresSettings,
    load_settings,
)
from telemetry_core.errors import ConfigurationError


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Run with no ambient .env and no inherited platform variables."""
    monkeypatch.chdir(tmp_path)
    for name in (
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_SECURITY_PROTOCOL",
        "KAFKA_CLIENT_ID",
        "KAFKA_TOPIC_TELEMETRY_RAW",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "LOG_LEVEL",
        "SERVICE_NAME",
    ):
        monkeypatch.delenv(name, raising=False)


class TestRequiredValues:
    def test_missing_broker_fails_at_startup(self) -> None:
        """No default for a broker address: a default is a hard-coded broker."""
        with pytest.raises(ConfigurationError) as info:
            load_settings(KafkaSettings)
        assert "KAFKA_BOOTSTRAP_SERVERS" in str(info.value)

    def test_error_names_every_missing_variable(self) -> None:
        """At startup, an operator needs the list of variables, not a stack trace."""
        with pytest.raises(ConfigurationError) as info:
            load_settings(PostgresSettings)
        message = str(info.value)
        for expected in ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"):
            assert expected in message

    def test_broker_present_is_enough(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        settings = load_settings(KafkaSettings)
        assert settings.bootstrap_servers == "kafka:9092"
        assert settings.security_protocol == "PLAINTEXT"

    def test_empty_string_is_not_a_valid_broker(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "")
        with pytest.raises(ConfigurationError):
            load_settings(KafkaSettings)


class TestTopicDefaults:
    def test_topics_default_to_the_contract_names(self) -> None:
        """Topic names belong to the versioned design, not to the environment."""
        topics = load_settings(KafkaTopics)
        assert topics.telemetry_raw == "telemetry.raw"
        assert topics.alerts == "alerts"
        assert topics.telemetry_dlq == "telemetry.dlq"

    def test_topics_remain_overridable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A shared cluster may need a per-environment prefix."""
        monkeypatch.setenv("KAFKA_TOPIC_TELEMETRY_RAW", "staging.telemetry.raw")
        assert load_settings(KafkaTopics).telemetry_raw == "staging.telemetry.raw"

    def test_topic_variables_do_not_break_kafka_settings(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """KAFKA_ and KAFKA_TOPIC_ share a prefix namespace by design.

        Forbidding unknown keys would make KafkaSettings reject every topic
        variable, which is why extra="ignore" is the correct choice here.
        """
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        monkeypatch.setenv("KAFKA_TOPIC_TELEMETRY_RAW", "telemetry.raw")
        assert load_settings(KafkaSettings).bootstrap_servers == "kafka:9092"


class TestSecrets:
    @pytest.fixture(autouse=True)
    def _postgres_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_HOST", "postgres")
        monkeypatch.setenv("POSTGRES_DB", "anomaly")
        monkeypatch.setenv("POSTGRES_USER", "anomaly")
        monkeypatch.setenv("POSTGRES_PASSWORD", "s3cr3t-value")

    def test_password_is_masked_in_repr(self) -> None:
        """Guards the accidental leak: logging the settings object."""
        settings = load_settings(PostgresSettings)
        assert "s3cr3t-value" not in repr(settings)
        assert "s3cr3t-value" not in str(settings)

    def test_reading_the_secret_is_explicit(self) -> None:
        settings = load_settings(PostgresSettings)
        assert settings.password.get_secret_value() == "s3cr3t-value"

    def test_jdbc_url_carries_no_credentials(self) -> None:
        """Credentials in a URL end up in logs and process listings."""
        settings = load_settings(PostgresSettings)
        url = settings.jdbc_url
        assert url == "jdbc:postgresql://postgres:5432/anomaly"
        assert "s3cr3t-value" not in url
        assert "anomaly@" not in url

    def test_dsn_resolves_the_secret_when_asked(self) -> None:
        settings = load_settings(PostgresSettings)
        assert settings.dsn == "postgresql://anomaly:s3cr3t-value@postgres:5432/anomaly"

    def test_invalid_port_is_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_PORT", "70000")
        with pytest.raises(ConfigurationError):
            load_settings(PostgresSettings)


class TestImmutability:
    def test_settings_are_frozen(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Configuration that changes at runtime is configuration nobody can reason about."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        settings = load_settings(KafkaSettings)
        with pytest.raises((AttributeError, TypeError, ValueError)):
            settings.bootstrap_servers = "elsewhere:9092"


class TestLoggingSettings:
    def test_defaults_are_usable(self) -> None:
        settings = load_settings(LoggingSettings)
        assert settings.log_level == "INFO"

    def test_level_is_overridable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        assert load_settings(LoggingSettings).log_level == "DEBUG"


class TestEmptyVariablesMeanUnset:
    """Docker Compose expands an undefined variable to an empty string.

    ``FOO: ${FOO:-}`` in a compose file yields ``FOO=""`` in the container, not
    an absent variable. Treating that as a value makes every optional setting a
    startup failure -- which is exactly what happened: the committed
    ``.env.example`` leaves the optional variables blank, so the documented
    ``docker compose up`` command could not start the service at all.
    """

    def test_an_empty_optional_falls_back_to_its_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        monkeypatch.setenv("KAFKA_CLIENT_ID", "")

        settings = load_settings(KafkaSettings)

        assert settings.client_id == KafkaSettings.model_fields["client_id"].default

    def test_an_empty_required_still_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The rule must not turn a blank broker address into a silent default."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "")

        with pytest.raises(ConfigurationError) as info:
            load_settings(KafkaSettings)
        assert "KAFKA_BOOTSTRAP_SERVERS" in str(info.value)
