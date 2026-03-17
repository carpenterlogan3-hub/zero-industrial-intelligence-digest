"""BR_02_classify_article.py

For each unprocessed article, call REUSABLE_langchain_llm_call with:
    model=gpt-4o-mini, temperature=0.2, max_tokens=300
    system_prompt: config/prompts/classify_system.txt
    user_message: formatted article fields
    expect_json=True

Validates 4 required keys: topic_category, relevant_roles, importance, one_line_summary.
Validates enum values: topic in 5 categories, roles in 4 options, importance in 3 levels.

Exceptions:
    SE-01: API key invalid (401) → abort entire batch, alert admin.
    SE-02: Rate limit 429/5xx → per-article retry 5x with backoff, mark 'Error' on exhaust.
    BE-01: Malformed JSON → log raw response, mark 'Error', continue.
    BE-02: Invalid enum values → deterministic fallback (Other/MEDIUM/AI_IT), log warning.
"""

import logging
from pathlib import Path
from typing import Dict, List

from src.reusable.langchain_llm_call import llm_call

logger = logging.getLogger(__name__)

_PROMPT_PATH = Path(__file__).parent.parent.parent / "config" / "prompts" / "classify_system.txt"

_VALID_TOPIC_CATEGORIES = {"Regulatory", "AI/Tech", "Energy/TES", "Business/Finance", "Other"}
_VALID_ROLES = {"CEO", "SVP_Development_Canada", "VP_Finance", "AI_IT"}
_VALID_IMPORTANCE = {"HIGH", "MEDIUM", "LOW"}
_REQUIRED_KEYS = {"topic_category", "relevant_roles", "importance", "one_line_summary"}

# BE-02 deterministic fallback values
_FALLBACK_TOPIC = "Other"
_FALLBACK_IMPORTANCE = "MEDIUM"
_FALLBACK_ROLES = ["AI_IT"]


def _load_system_prompt() -> str:
    if not _PROMPT_PATH.exists():
        raise FileNotFoundError(f"Classify system prompt not found at {_PROMPT_PATH}")
    return _PROMPT_PATH.read_text(encoding="utf-8").strip()


def _format_user_message(article: Dict) -> str:
    return (
        f"Title: {article.get('title', '')}\n"
        f"Source: {article.get('source', '')}\n"
        f"Category: {article.get('feed_category', '')}\n"
        f"Published: {article.get('pub_date', '')}\n"
        f"URL: {article.get('url', '')}\n"
        f"Summary: {article.get('summary', '')}"
    )


def _validate_and_fix(classification: Dict, article_url: str) -> Dict:
    """Validate enum fields. Apply BE-02 fallback per-field where invalid."""
    fixed = dict(classification)
    warnings = []

    topic = fixed.get("topic_category", "")
    if topic not in _VALID_TOPIC_CATEGORIES:
        warnings.append(f"topic_category='{topic}'")
        fixed["topic_category"] = _FALLBACK_TOPIC

    importance = fixed.get("importance", "")
    if importance not in _VALID_IMPORTANCE:
        warnings.append(f"importance='{importance}'")
        fixed["importance"] = _FALLBACK_IMPORTANCE

    roles = fixed.get("relevant_roles", [])
    if not isinstance(roles, list):
        roles = [roles] if roles else []
    valid_roles = [r for r in roles if r in _VALID_ROLES]
    if not valid_roles:
        warnings.append(f"relevant_roles={roles}")
        valid_roles = _FALLBACK_ROLES
    fixed["relevant_roles"] = valid_roles

    if warnings:
        logger.warning(
            "BE-02: Invalid enum value(s) for %s — %s. Applied deterministic fallback.",
            article_url,
            ", ".join(warnings),
        )

    return fixed


def classify_articles(articles: List[Dict]) -> List[Dict]:
    """Classify each article via LLM. Returns only successfully classified articles.

    Each returned dict contains all original fields plus:
        topic_category, relevant_roles (list), importance, one_line_summary

    Articles that fail with BE-01/SE-02 are logged and excluded from output.
    SE-01 (auth failure) aborts the entire batch immediately.
    """
    system_prompt = _load_system_prompt()
    classified: List[Dict] = []
    error_count = 0

    for i, article in enumerate(articles):
        url = article.get("url", f"article_{i}")
        user_message = _format_user_message(article)

        try:
            result = llm_call(
                system_prompt=system_prompt,
                user_message=user_message,
                model_name="gpt-4o-mini",
                temperature=0.2,
                max_tokens=300,
                expect_json=True,
            )
        except RuntimeError as exc:
            error_msg = str(exc)
            if "SE-01" in error_msg:
                logger.error("SE-01: API auth failure — aborting entire classification batch: %s", exc)
                raise
            else:
                # SE-02: rate limit exhausted after llm_call's internal 5 retries
                logger.error("SE-02: Rate limit/server error exhausted for %s: %s — marking Error.", url, exc)
                error_count += 1
                continue
        except Exception as exc:
            # BE-01: covers json.JSONDecodeError from expect_json=True parsing
            logger.error(
                "BE-01: Failed to parse LLM response for %s: %s — marking Error, continuing.",
                url, exc,
            )
            error_count += 1
            continue

        # Validate required keys
        missing_keys = _REQUIRED_KEYS - set(result.keys())
        if missing_keys:
            logger.error(
                "BE-01: LLM response for %s missing required keys %s. Raw: %s — marking Error.",
                url, missing_keys, result,
            )
            error_count += 1
            continue

        # Validate + fix enum values (BE-02)
        result = _validate_and_fix(result, url)

        enriched = dict(article)
        enriched["topic_category"] = result["topic_category"]
        enriched["relevant_roles"] = result["relevant_roles"]
        enriched["importance"] = result["importance"]
        enriched["one_line_summary"] = str(result.get("one_line_summary", ""))[:200]
        classified.append(enriched)
        logger.debug("Classified %d/%d: %s → %s/%s", i + 1, len(articles), url,
                     result["topic_category"], result["importance"])

    logger.info(
        "Classification complete: %d/%d succeeded, %d error(s).",
        len(classified), len(articles), error_count,
    )
    return classified
