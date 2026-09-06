"""Independent random streams derived from a single master seed.

This is guard 3 of docs/07-ml-methodology.md section 1, made mechanical.

If noise and anomaly injection drew from one shared generator, enabling an
anomaly would consume draws and **shift the entire noise sequence**. The noise
would then correlate with the injection, and a model could learn that
correlation instead of the anomaly. The result looks excellent offline and
means nothing.

Every stream is therefore derived from ``(purpose, key)`` rather than from a
shared cursor:

* changing the anomaly configuration cannot move the noise sequence;
* adding or reordering machines cannot change the data of the others, because
  streams are keyed by machine identity rather than by index.

``zlib.crc32`` provides the key hash rather than ``hash()``: Python randomises
string hashing per process by default, which would make runs irreproducible in
the one place reproducibility is the entire point.
"""

from __future__ import annotations

import secrets
from zlib import crc32

import numpy as np

__all__ = ["RngRegistry", "resolve_seed"]

_MAX_SEED = 2**63 - 1


def resolve_seed(seed: int | None) -> int:
    """Return ``seed``, or draw one when it is absent.

    A drawn seed is a real value that the caller logs, so any run can be
    reproduced afterwards. Leaving the seed implicit would make an interesting
    run impossible to replay.
    """
    if seed is not None:
        if not 0 <= seed <= _MAX_SEED:
            raise ValueError(f"seed must fit in [0, {_MAX_SEED}], got {seed}")
        return seed
    return secrets.randbelow(_MAX_SEED)


class RngRegistry:
    """Named, independent generators derived from one master seed."""

    __slots__ = ("_cache", "_seed")

    def __init__(self, seed: int) -> None:
        self._seed = seed
        self._cache: dict[tuple[str, str], np.random.Generator] = {}

    @property
    def seed(self) -> int:
        return self._seed

    def stream(self, purpose: str, key: str = "") -> np.random.Generator:
        """Return the generator for ``(purpose, key)``, creating it on first use.

        Memoised: a generator is stateful, so returning a fresh one on every call
        would restart the sequence and destroy both independence and progress.
        """
        cache_key = (purpose, key)
        generator = self._cache.get(cache_key)
        if generator is None:
            sequence = np.random.SeedSequence(
                entropy=self._seed,
                spawn_key=(crc32(purpose.encode("utf-8")), crc32(key.encode("utf-8"))),
            )
            generator = np.random.default_rng(sequence)
            self._cache[cache_key] = generator
        return generator
