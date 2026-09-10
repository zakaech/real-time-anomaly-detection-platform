#!/usr/bin/env python3
"""Keeps .env.example and the compose file honest about each other.

Two failures this catches, both silent:

* a variable declared in ``.env.example`` that nothing interpolates. It reads
  like configuration, someone changes it, and nothing happens. Phase 6 found
  two: ``KAFKA_EXTERNAL_BOOTSTRAP``, never referenced anywhere, and
  ``POSTGRES_PORT``, which was worse than useless because the JDBC URL had the
  port written into it -- editing the variable changed nothing at all, while
  looking like it should.

* a variable compose needs that the template does not declare. ``docker compose
  config`` already fails on that one, but only for variables with no fallback;
  a ``${FOO:-default}`` added to compose and forgotten here passes validation
  and then quietly runs on the default forever.

Run it directly, through ``make check-env``, or in CI. Needs no dependency
beyond the standard library, which is why it is a script and not a test in one
of the language toolchains: it is about two files, not about any component.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_EXAMPLE = REPO_ROOT / ".env.example"
COMPOSE_FILE = REPO_ROOT / "infra" / "docker-compose.yml"

DECLARATION = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=")
# ${VAR}, ${VAR:-default} and ${VAR-default} all count as a use.
INTERPOLATION = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)")


def declared_variables(path: Path) -> list[str]:
    names: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = DECLARATION.match(stripped)
        if match:
            names.append(match.group(1))
    return names


def interpolated_variables(path: Path) -> set[str]:
    return set(INTERPOLATION.findall(path.read_text(encoding="utf-8")))


def main() -> int:
    for path in (ENV_EXAMPLE, COMPOSE_FILE):
        if not path.exists():
            print(f"FAIL  missing file: {path}")
            return 1

    declared = declared_variables(ENV_EXAMPLE)
    used = interpolated_variables(COMPOSE_FILE)

    duplicates = sorted({name for name in declared if declared.count(name) > 1})
    dead = [name for name in declared if name not in used]
    undeclared = sorted(name for name in used if name not in declared)

    print(f"declared in .env.example : {len(declared)}")
    print(f"used by docker-compose   : {len(used)}")

    failed = False

    if duplicates:
        failed = True
        print("\nFAIL  declared more than once (the last one silently wins):")
        for name in duplicates:
            print(f"        {name}")

    if dead:
        failed = True
        print("\nFAIL  declared but never used -- remove it, or wire it up:")
        for name in dead:
            print(f"        {name}")

    if undeclared:
        failed = True
        print("\nFAIL  used by compose but absent from .env.example:")
        for name in undeclared:
            print(f"        {name}")
        print("      A variable with a fallback runs on its default for ever")
        print("      and `docker compose config` will not complain.")

    if failed:
        return 1

    print("\nOK    every declared variable is used, and every used one is declared.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
