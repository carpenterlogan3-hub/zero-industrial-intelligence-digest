"""BR_03_role_distribution_config.py

Load config/distribution_config.yaml. Validate each stakeholder has non-empty
email and prompt_template_file that exists on disk.

Returns: List[Dict] — validated stakeholder config entries.

Exceptions:
    BE-01: Missing required fields → log warning, skip invalid stakeholders, continue with valid.
"""

import logging
from pathlib import Path
from typing import Dict, List

import yaml

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "distribution_config.yaml"
_PROJECT_ROOT = Path(__file__).parent.parent.parent


def load_distribution_config() -> List[Dict]:
    """Load and validate config/distribution_config.yaml.

    Returns only stakeholders that pass all validation checks.
    Invalid entries are logged and skipped (BE-01) — pipeline continues with valid ones.
    """
    if not _CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"distribution_config.yaml not found at {_CONFIG_PATH}. Aborting pipeline."
        )

    with open(_CONFIG_PATH, "r") as fh:
        raw = yaml.safe_load(fh)

    if not isinstance(raw, dict) or "stakeholders" not in raw:
        raise ValueError(
            "distribution_config.yaml must contain a top-level 'stakeholders' key."
        )

    stakeholders_raw = raw["stakeholders"]
    if not isinstance(stakeholders_raw, list):
        raise ValueError("'stakeholders' in distribution_config.yaml must be a list.")

    valid: List[Dict] = []
    for entry in stakeholders_raw:
        name = entry.get("name", "<unnamed>")
        issues = []

        if not entry.get("email", "").strip():
            issues.append("missing 'email'")

        if not entry.get("name", "").strip():
            issues.append("missing 'name'")

        template_path_str = entry.get("prompt_template_file", "")
        if not template_path_str:
            issues.append("missing 'prompt_template_file'")
        else:
            # Resolve relative to project root
            template_path = _PROJECT_ROOT / template_path_str
            if not template_path.exists():
                issues.append(
                    f"prompt_template_file '{template_path_str}' not found on disk"
                )

        if issues:
            logger.warning(
                "BE-01: Skipping stakeholder '%s' — %s.", name, "; ".join(issues)
            )
            continue

        valid.append(entry)

    logger.info(
        "Loaded %d/%d valid stakeholder(s) from distribution_config.yaml.",
        len(valid), len(stakeholders_raw),
    )
    return valid
