from __future__ import annotations

import unittest
from unittest.mock import patch

import src.live.topics as live_topics


class LiveTopicsTests(unittest.TestCase):
    def test_kafka_raw_topic_uses_default_prefix(self) -> None:
        self.assertEqual(
            live_topics.kafka_raw_topic("boston"),
            "transit.live.raw.boston.vehicles",
        )

    def test_kafka_latest_topic_uses_default_prefix(self) -> None:
        self.assertEqual(
            live_topics.kafka_latest_topic("chicago"),
            "transit.live.latest.chicago.vehicles",
        )

    def test_topic_helpers_respect_overridden_prefix_constant(self) -> None:
        with patch.object(live_topics, "KAFKA_TOPIC_PREFIX", "demo.prefix"):
            self.assertEqual(
                live_topics.kafka_raw_topic("boston"),
                "demo.prefix.raw.boston.vehicles",
            )
            self.assertEqual(
                live_topics.kafka_latest_topic("chicago"),
                "demo.prefix.latest.chicago.vehicles",
            )


if __name__ == "__main__":
    unittest.main()
