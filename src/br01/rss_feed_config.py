"""BR_01_rss_feed_config.py

Load config/feeds.yaml and validate its structure. Returns a flat list of feed
dicts: [{name, url, feed_category}, ...] with exactly 19 entries across 4 categories
(regulatory=5, ai_tech=5, energy_tes=5, business_finance=4).

Does NOT make HTTP calls — reachability is validated in fetch_rss_feeds.py.

Exceptions:
    SE-01: config/feeds.yaml missing or invalid YAML → abort pipeline, alert admin.
    BE-01: Valid YAML but missing category keys or zero feed URLs → abort pipeline, alert admin.
"""

import logging
from pathlib import Path
from typing import Dict, List

import yaml

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "feeds.yaml"

_REQUIRED_CATEGORIES: Dict[str, int] = {
    "regulatory": 5,
    "ai_tech": 5,
    "energy_tes": 5,
    "business_finance": 4,
}

_CATEGORY_DISPLAY: Dict[str, str] = {
    "regulatory": "Regulatory",
    "ai_tech": "AI/Tech",
    "energy_tes": "Energy/TES",
    "business_finance": "Business/Finance",
}


def load_feed_config() -> List[Dict]:
    """Load and validate config/feeds.yaml.

    Returns a flat list of dicts with keys: name, url, feed_category.
    Raises on missing file, invalid YAML, missing categories, or empty feed lists.
    """
    # SE-01: file missing or invalid YAML
    if not _CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"SE-01: config/feeds.yaml not found at {_CONFIG_PATH}. Aborting pipeline."
        )

    try:
        with open(_CONFIG_PATH, "r") as fh:
            raw = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        raise ValueError(f"SE-01: config/feeds.yaml is invalid YAML: {exc}") from exc

    if not isinstance(raw, dict):
        raise ValueError("SE-01: config/feeds.yaml must be a YAML mapping at the top level.")

    # BE-01: missing category keys or empty feed lists
    missing = [cat for cat in _REQUIRED_CATEGORIES if cat not in raw]
    if missing:
        raise ValueError(
            f"BE-01: config/feeds.yaml missing required category keys: {missing}. "
            "Aborting pipeline."
        )

    feeds: List[Dict] = []
    for category, expected_count in _REQUIRED_CATEGORIES.items():
        entries = raw[category]
        if not entries or not isinstance(entries, list):
            raise ValueError(
                f"BE-01: Category '{category}' has no feed entries in feeds.yaml. "
                "Aborting pipeline."
            )
        for entry in entries:
            if not entry.get("url"):
                raise ValueError(
                    f"BE-01: Feed '{entry.get('name', '?')}' in category '{category}' "
                    "is missing a URL. Aborting pipeline."
                )
            feeds.append({
                "name": entry["name"],
                "url": entry["url"],
                "feed_category": _CATEGORY_DISPLAY[category],
            })

        actual = len(entries)
        if actual != expected_count:
            logger.warning(
                "feeds.yaml category '%s' has %d entries (expected %d).",
                category,
                actual,
                expected_count,
            )

    logger.info("Loaded %d feeds across %d categories.", len(feeds), len(_REQUIRED_CATEGORIES))
    return feeds
