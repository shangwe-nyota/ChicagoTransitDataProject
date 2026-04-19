from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql.functions import asin, cos, lit, radians, sin, sqrt


EARTH_RADIUS_M = 6_371_000


def haversine_distance(
    lat1: Column,
    lon1: Column,
    lat2: Column,
    lon2: Column,
) -> Column:
    lat1_r = radians(lat1)
    lat2_r = radians(lat2)
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(lat1_r) * cos(lat2_r) * sin(dlon / 2) ** 2
    return lit(2 * EARTH_RADIUS_M) * asin(sqrt(a))
