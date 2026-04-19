from __future__ import annotations

import unittest

from src.common.config import get_batch_city_config


class BatchConfigTests(unittest.TestCase):
    def test_get_batch_city_config_returns_expected_boston_values(self) -> None:
        config = get_batch_city_config("boston")
        self.assertEqual(config.slug, "boston")
        self.assertIn("MBTA_GTFS.zip", config.gtfs_static_url)
        self.assertEqual(config.osm_place_name, "Boston, Massachusetts, USA")

    def test_get_batch_city_config_is_case_insensitive(self) -> None:
        config = get_batch_city_config("Chicago")
        self.assertEqual(config.slug, "chicago")

    def test_get_batch_city_config_raises_for_unknown_city(self) -> None:
        with self.assertRaises(KeyError):
            get_batch_city_config("london")


if __name__ == "__main__":
    unittest.main()
