"""Typed configuration, validated at startup.

Nothing in this platform reads ``os.environ`` directly. Every component builds a
settings object here, and a missing or malformed value raises before the first
message is processed.

That timing matters. A component that starts with a partial configuration and
fails on the first message fails in production, under load,
with a stack trace pointing at the message rather than at the deployment. One
that refuses to start fails in the deployment pipeline, where it is cheap.

Which values carry a default, and which do not, follows one rule:

* **No default** for anything bound to an environment or to a secret -- broker
  addresses, database host, credentials. The brief requires no hard-coded broker
  and no hard-coded secret; a default *is* a hard-coded value that merely stays
  hidden until the variable is misspelled.
* **A default** for topic names, which belong to the versioned design rather
  than to the environment. They stay overridable so a shared cluster can prefix
  them per environment.
"""

from __future__ import annotations

from typing import TypeVar

from pydantic import Field, SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from telemetry_core.errors import ConfigurationError

__all__ = [
    "KafkaSettings",
    "KafkaTopics",
    "LoggingSettings",
    "PostgresSettings",
    "load_settings",
]

_SettingsT = TypeVar("_SettingsT", bound=BaseSettings)


def _settings_config(prefix: str) -> SettingsConfigDict:
    """Shared settings behaviour, parameterised by environment prefix.

    ``extra="ignore"`` rather than ``"forbid"``: the prefixes overlap by design
    (``KAFKA_`` and ``KAFKA_TOPIC_``), so forbidding unknown keys would make
    ``KafkaSettings`` reject every ``KAFKA_TOPIC_*`` variable that legitimately
    belongs to ``KafkaTopics``. Protection against a misspelled variable comes
    from required fields instead: omit one and startup fails.
    """
    return SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix=prefix,
        extra="ignore",
        frozen=True,
        # An empty variable means "unset", not "invalid". Compose expands an
        # undefined variable to an empty string -- `FOO: ${FOO:-}` yields
        # FOO="" -- so without this an optional field like a seed or a run
        # duration fails validation and the container refuses to start, on
        # exactly the command the README documents.
        env_ignore_empty=True,
    )


class KafkaSettings(BaseSettings):
    """Connection settings for every Kafka client in the platform."""

    model_config = _settings_config("KAFKA_")

    bootstrap_servers: str = Field(
        ...,
        min_length=1,
        description=(
            "Comma-separated broker list. Required on purpose: a default would be "
            "a hard-coded broker, which is exactly what must not exist."
        ),
    )
    security_protocol: str = Field(default="PLAINTEXT")
    client_id: str | None = Field(default=None)


class KafkaTopics(BaseSettings):
    """Topic names, defaulting to the canonical names of the data contract.

    Defaults are acceptable here because a topic name is part of the design and
    is versioned in this repository -- unlike a broker address, which belongs to
    the deployment environment.
    """

    model_config = _settings_config("KAFKA_TOPIC_")

    telemetry_raw: str = Field(default="telemetry.raw", min_length=1)
    telemetry_scored: str = Field(default="telemetry.scored", min_length=1)
    alerts: str = Field(default="alerts", min_length=1)
    telemetry_labels: str = Field(default="telemetry.labels", min_length=1)
    telemetry_dlq: str = Field(default="telemetry.dlq", min_length=1)
    telemetry_late: str = Field(default="telemetry.late", min_length=1)


class PostgresSettings(BaseSettings):
    """PostgreSQL connection settings.

    The password is a :class:`~pydantic.SecretStr`, so it is masked in ``repr``
    and in any structured log line that happens to serialise the settings object.
    Reading it requires an explicit ``get_secret_value()`` call, which turns
    every potential leak into a deliberate, greppable act.
    """

    model_config = _settings_config("POSTGRES_")

    host: str = Field(..., min_length=1)
    port: int = Field(default=5432, ge=1, le=65535)
    db: str = Field(..., min_length=1)
    user: str = Field(..., min_length=1)
    password: SecretStr = Field(...)

    @property
    def jdbc_url(self) -> str:
        """JDBC URL, deliberately credential-free.

        Credentials travel as separate connection properties. Embedding them in
        a URL is how they end up in logs, exception messages and process
        listings.
        """
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"

    @property
    def dsn(self) -> str:
        """DSN for Python clients, with the password resolved."""
        secret = self.password.get_secret_value()
        return f"postgresql://{self.user}:{secret}@{self.host}:{self.port}/{self.db}"


class LoggingSettings(BaseSettings):
    """Structured logging settings."""

    model_config = _settings_config("")

    log_level: str = Field(default="INFO")
    service_name: str = Field(default="unknown-service", min_length=1)
    service_version: str = Field(default="0.0.0", min_length=1)


def load_settings(settings_type: type[_SettingsT]) -> _SettingsT:
    """Instantiate a settings class, turning validation failures into a clear error.

    Pydantic's ``ValidationError`` is precise but verbose. At startup the only
    thing an operator needs is which variables are missing or wrong, named as
    they appear in the environment.

    Raises:
        ConfigurationError: if any required value is missing or invalid.
    """
    try:
        return settings_type()
    except ValidationError as exc:
        prefix = str(settings_type.model_config.get("env_prefix", ""))
        problems = "; ".join(
            f"{prefix}{'_'.join(str(part) for part in error['loc']).upper()}: {error['msg']}"
            for error in exc.errors()
        )
        raise ConfigurationError(
            f"invalid configuration for {settings_type.__name__}: {problems}"
        ) from exc
