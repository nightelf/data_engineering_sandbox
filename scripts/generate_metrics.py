"""Generate synthetic click/call/impression metric data for mongoimport.

Writes 100,000 records to data/metrics.json as a pretty-printed JSON array
(human readable). Import with `mongoimport --jsonArray`.
The date_time field uses MongoDB Extended JSON ({"$date": ...}) so mongoimport
parses it as a BSON Date.
"""

import json
import random
import uuid
from datetime import datetime, timezone

NUM_RECORDS = 100_000
OUTPUT_PATH = "/opt/spark/work-dir/data/metrics.json"

# Weighted distributions produce a realistic long-tail spread in group counts
# (rather than every group landing near the same value). Weights are relative.
METRIC_TYPES = ["click", "call", "impression"]
METRIC_WEIGHTS = [25, 5, 70]  # funnel-like: impressions >> clicks >> calls

WEBSITES = ["example.com", "sample.com", "filler.com"]
WEBSITE_WEIGHTS = [55, 30, 15]

DEVICES = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS = [45, 50, 5]

COUNTRIES = ["US", "CA", "UK", "DE", "AU"]
COUNTRY_WEIGHTS = [50, 15, 15, 12, 8]

BROWSERS = ["chrome", "safari", "firefox", "edge"]
BROWSER_WEIGHTS = [60, 22, 12, 6]

CAMPAIGN_IDS = [1, 2, 3, 4, 5]
CAMPAIGN_WEIGHTS = [40, 25, 18, 12, 5]

USER_ID_RANGE = (1, 10_000)

# Date range: June 1, 2026 -> September 19, 2026. Weighted by month so the
# monthly buckets differ noticeably in volume.
START_TS = datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp()
END_TS = datetime(2026, 9, 19, 23, 59, 59, tzinfo=timezone.utc).timestamp()
MONTH_BOUNDS = [
    (datetime(2026, 6, 1, tzinfo=timezone.utc), datetime(2026, 7, 1, tzinfo=timezone.utc)),
    (datetime(2026, 7, 1, tzinfo=timezone.utc), datetime(2026, 8, 1, tzinfo=timezone.utc)),
    (datetime(2026, 8, 1, tzinfo=timezone.utc), datetime(2026, 9, 1, tzinfo=timezone.utc)),
    (datetime(2026, 9, 1, tzinfo=timezone.utc), datetime(2026, 9, 20, tzinfo=timezone.utc)),
]
MONTH_WEIGHTS = [40, 30, 20, 10]  # June heaviest, tapering to a partial September

# Franchise popularity skew — a few dominate the referrer traffic.
FRANCHISE_WEIGHTS = {
    "star-wars": 35,
    "lord-of-the-rings": 25,
    "harry-potter": 18,
    "star-trek": 10,
    "dune": 8,
    "the-matrix": 4,
}

# Referrer assembly (type B): franchise-coherent items, varying depth.
FRANCHISES = {
    "star-wars": {
        "characters": ["luke-skywalker", "darth-vader", "leia-organa", "yoda", "han-solo"],
        "planets": ["tatooine", "hoth", "endor", "naboo", "coruscant"],
        "ships": ["millennium-falcon", "x-wing", "star-destroyer", "tie-fighter"],
        "factions": ["rebel-alliance", "galactic-empire", "jedi-order", "sith"],
    },
    "lord-of-the-rings": {
        "characters": ["frodo-baggins", "gandalf", "aragorn", "legolas", "gollum"],
        "places": ["the-shire", "mordor", "rivendell", "gondor", "rohan"],
        "weapons": ["the-one-ring", "sting", "anduril", "glamdring"],
        "factions": ["fellowship", "orcs", "elves", "ents"],
    },
    "dune": {
        "characters": ["paul-atreides", "lady-jessica", "duncan-idaho", "baron-harkonnen"],
        "planets": ["arrakis", "caladan", "giedi-prime", "salusa-secundus"],
        "factions": ["house-atreides", "house-harkonnen", "fremen", "bene-gesserit"],
        "creatures": ["sandworm", "sandtrout"],
    },
    "star-trek": {
        "characters": ["jean-luc-picard", "spock", "james-kirk", "data", "worf"],
        "ships": ["enterprise", "voyager", "defiant", "discovery"],
        "factions": ["starfleet", "klingon-empire", "romulans", "borg"],
        "planets": ["vulcan", "qonos", "risa", "bajor"],
    },
    "the-matrix": {
        "characters": ["neo", "morpheus", "trinity", "agent-smith"],
        "places": ["the-matrix", "zion", "the-construct"],
        "factions": ["the-resistance", "machines", "programs"],
    },
    "harry-potter": {
        "characters": ["harry-potter", "hermione-granger", "ron-weasley", "albus-dumbledore", "voldemort"],
        "places": ["hogwarts", "diagon-alley", "the-burrow", "azkaban"],
        "factions": ["gryffindor", "slytherin", "death-eaters", "order-of-the-phoenix"],
        "creatures": ["dementor", "hippogriff", "basilisk"],
    },
}


def weighted(values, weights):
    return random.choices(values, weights=weights, k=1)[0]


FRANCHISE_NAMES = list(FRANCHISE_WEIGHTS.keys())
FRANCHISE_W = list(FRANCHISE_WEIGHTS.values())


def random_referrer():
    franchise = weighted(FRANCHISE_NAMES, FRANCHISE_W)
    categories = FRANCHISES[franchise]
    category = random.choice(list(categories.keys()))
    # Vary depth: sometimes /franchise/category/, sometimes /franchise/category/item/
    if random.random() < 0.6:
        item = random.choice(categories[category])
        return f"/{franchise}/{category}/{item}/"
    return f"/{franchise}/{category}/"


def random_datetime():
    # Pick a month bucket by weight, then a uniform instant within it.
    start, end = weighted(MONTH_BOUNDS, MONTH_WEIGHTS)
    ts = random.uniform(start.timestamp(), end.timestamp())
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def random_value(metric_type):
    # Unified numeric field across all metric types.
    if metric_type == "impression":
        return 1
    if metric_type == "click":
        return round(random.uniform(0.10, 2.50), 2)  # cost-per-click
    return random.randint(30, 600)  # call duration in seconds


def make_record():
    metric_type = weighted(METRIC_TYPES, METRIC_WEIGHTS)
    dt = random_datetime()
    return {
        "metric_type": metric_type,
        "website": weighted(WEBSITES, WEBSITE_WEIGHTS),
        "referrer": random_referrer(),
        "campaign_id": weighted(CAMPAIGN_IDS, CAMPAIGN_WEIGHTS),
        "date_time": {"$date": dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")},
        "value": random_value(metric_type),
        "device": weighted(DEVICES, DEVICE_WEIGHTS),
        "country": weighted(COUNTRIES, COUNTRY_WEIGHTS),
        "user_id": random.randint(*USER_ID_RANGE),
        "session_id": str(uuid.uuid4()),
        "browser": weighted(BROWSERS, BROWSER_WEIGHTS),
    }


def main():
    records = [make_record() for _ in range(NUM_RECORDS)]
    with open(OUTPUT_PATH, "w") as f:
        json.dump(records, f, indent=2)
    print(f"Wrote {NUM_RECORDS:,} records to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
