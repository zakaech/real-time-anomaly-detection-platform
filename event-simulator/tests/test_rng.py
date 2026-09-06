"""Independence of the random streams.

The last test in this file is the one that matters. If noise and anomaly
injection shared a generator, enabling anomalies would consume draws and shift
the whole noise sequence. The noise would then correlate with the injection, a
model could learn that correlation instead of the anomaly, and the offline
metrics would look excellent while meaning nothing.
"""

from __future__ import annotations

import pytest

from event_simulator.generation.rng import RngRegistry, resolve_seed


class TestSeedResolution:
    def test_explicit_seed_is_returned(self) -> None:
        assert resolve_seed(1234) == 1234

    def test_absent_seed_is_drawn(self) -> None:
        """Drawn, not left implicit: the caller logs it so a run can be replayed."""
        first = resolve_seed(None)
        second = resolve_seed(None)
        assert isinstance(first, int)
        assert first != second

    def test_out_of_range_seed_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="seed must fit"):
            resolve_seed(-1)


class TestStreams:
    def test_same_seed_yields_the_same_values(self) -> None:
        first = RngRegistry(7).stream("noise", "M-001").normal(size=5)
        second = RngRegistry(7).stream("noise", "M-001").normal(size=5)
        assert list(first) == list(second)

    def test_different_seeds_diverge(self) -> None:
        first = RngRegistry(7).stream("noise", "M-001").normal(size=5)
        second = RngRegistry(8).stream("noise", "M-001").normal(size=5)
        assert list(first) != list(second)

    def test_streams_are_memoised(self) -> None:
        """A fresh generator per call would restart the sequence and never advance."""
        registry = RngRegistry(7)
        assert registry.stream("noise", "M-001") is registry.stream("noise", "M-001")

    def test_purposes_are_independent(self) -> None:
        registry = RngRegistry(7)
        noise = registry.stream("noise", "M-001").normal(size=5)
        anomaly = registry.stream("anomaly", "M-001").normal(size=5)
        assert list(noise) != list(anomaly)

    def test_machines_are_independent(self) -> None:
        registry = RngRegistry(7)
        first = registry.stream("noise", "M-001").normal(size=5)
        second = registry.stream("noise", "M-002").normal(size=5)
        assert list(first) != list(second)

    def test_machine_streams_are_keyed_by_identity_not_order(self) -> None:
        """Adding or reordering machines must not change another machine's data."""
        early = RngRegistry(7)
        early.stream("noise", "M-001").normal(size=3)

        late = RngRegistry(7)
        # Draw from other machines first; M-001 must be unaffected.
        late.stream("noise", "M-009").normal(size=50)
        late.stream("noise", "M-002").normal(size=50)

        assert list(RngRegistry(7).stream("noise", "M-001").normal(size=3)) == list(
            late.stream("noise", "M-001").normal(size=3)
        )

    def test_anomaly_draws_cannot_shift_the_noise_sequence(self) -> None:
        """Guard 3 of docs/07 section 1, enforced mechanically.

        A shared generator would make the noise depend on how many anomaly draws
        happened, which is a signal a model could learn instead of the anomaly.
        """
        untouched = RngRegistry(11)
        expected = list(untouched.stream("noise", "M-004").normal(size=10))

        disturbed = RngRegistry(11)
        anomaly_stream = disturbed.stream("anomaly", "M-004")
        for _ in range(1000):
            anomaly_stream.random()
        actual = list(disturbed.stream("noise", "M-004").normal(size=10))

        assert actual == expected
