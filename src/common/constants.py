from __future__ import annotations


OSM_RELEVANT_HIGHWAY_TYPES = [
    "motorway",
    "trunk",
    "primary",
    "secondary",
    "tertiary",
    "residential",
    "unclassified",
    "service",
]

OSM_RELEVANT_POI_CATEGORY_MAP: dict[str, str] = {
    "restaurant": "food",
    "fast_food": "food",
    "cafe": "food",
    "school": "education",
    "university": "education",
    "college": "education",
    "hospital": "healthcare",
    "clinic": "healthcare",
    "pharmacy": "healthcare",
    "doctors": "healthcare",
    "supermarket": "grocery",
    "convenience": "grocery",
    "greengrocer": "grocery",
    "library": "civic",
    "bank": "civic",
    "post_office": "civic",
    "community_centre": "civic",
    "police": "civic",
    "fire_station": "civic",
    "place_of_worship": "civic",
    "park": "park",
    "playground": "park",
    "garden": "park",
    "stadium": "entertainment",
    "museum": "entertainment",
}

OSM_POI_TAGS = {
    "amenity": [
        "restaurant",
        "fast_food",
        "cafe",
        "school",
        "university",
        "college",
        "hospital",
        "clinic",
        "pharmacy",
        "doctors",
        "library",
        "bank",
        "post_office",
        "community_centre",
        "police",
        "fire_station",
        "place_of_worship",
    ],
    "shop": [
        "supermarket",
        "convenience",
        "greengrocer",
    ],
    "leisure": [
        "park",
        "playground",
        "garden",
    ],
    "tourism": [
        "museum",
    ],
    "building": [
        "stadium",
    ],
}

STOP_POI_ACCESS_DISTANCE_M = 400
TRANSIT_ROAD_COVERAGE_DISTANCE_M = 200
