"""BR_02_classify_article.py — V2

For each unprocessed article, call REUSABLE_langchain_llm_call with:
    model=gpt-4o-mini, temperature=0.2, max_tokens=300
    system_prompt: config/prompts/classify_system.txt
    user_message: formatted article fields
    expect_json=True

V2 CHANGES:
- JSON response now uses "relevant_persons" (list of full names) instead of "relevant_roles"
- importance can be "HIGH", "MEDIUM", or "SKIP"
- Articles classified as "SKIP" are NOT stored in Classified Items tab — they are discarded
- Only HIGH and MEDIUM articles proceed to store_classified.py
- Validates: topic_category in 5 categories, importance in [HIGH, MEDIUM, SKIP],
  relevant_persons is a non-empty list of strings

Exceptions:
    SE-01: API key invalid (401) → abort entire batch, alert admin.
    SE-02: Rate limit 429/5xx → per-article retry 5x with 30s waits, mark 'Error' on exhaust.
    BE-01: Malformed JSON → log raw response, mark 'Error', continue.
    BE-02: Invalid enum values → topic defaults to Other, importance defaults to MEDIUM.
"""

import logging
import time
from pathlib import Path
from typing import Dict, List

from src.reusable.langchain_llm_call import llm_call

logger = logging.getLogger(__name__)

_PROMPT_PATH = Path(__file__).parent.parent.parent / "config" / "prompts" / "classify_system.txt"

_VALID_TOPIC_CATEGORIES = {"Regulatory", "AI/Tech", "Energy/TES", "Business/Finance", "Other"}
_VALID_IMPORTANCE = {"HIGH", "MEDIUM", "LOW"}
_REQUIRED_KEYS = {"topic_category", "relevant_persons", "importance", "one_line_summary"}

_FALLBACK_TOPIC = "Other"
_FALLBACK_IMPORTANCE = "MEDIUM"

_INTER_ARTICLE_DELAY = 5    # seconds between each article
_RETRY_WAIT = 30            # seconds to wait after any failed attempt
_MAX_ATTEMPTS = 5           # max attempts per article


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
    """Validate enum fields. Apply BE-02 fallback where invalid."""
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

    # Normalise relevant_persons to a list of non-empty strings
    persons = fixed.get("relevant_persons", [])
    if not isinstance(persons, list):
        persons = [str(persons)] if persons else []
    persons = [str(p).strip() for p in persons if str(p).strip()]
    if not persons:
        warnings.append("relevant_persons=[] — defaulting to Ted Kniesche")
        persons = ["Ted Kniesche"]
    fixed["relevant_persons"] = persons

    if warnings:
        logger.warning(
            "BE-02: Invalid value(s) for %s — %s. Applied fallback.",
            article_url,
            ", ".join(warnings),
        )

    return fixed


def _call_with_retry(system_prompt: str, user_message: str, url: str) -> Dict:
    """Call llm_call with up to 5 attempts, waiting 30s between failures.

    Raises RuntimeError("SE-01:...") immediately on auth failure.
    Raises RuntimeError("SE-02:...") after all attempts exhausted.
    Raises ValueError on JSON parse failure (BE-01).
    """
    last_exc = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            result = llm_call(
                system_prompt=system_prompt,
                user_message=user_message,
                model_name="gpt-4o-mini",
                temperature=0.2,
                max_tokens=300,
                expect_json=True,
            )
            return result
        except RuntimeError as exc:
            if "SE-01" in str(exc):
                raise  # auth failure — abort immediately, no retry
            last_exc = exc
            logger.warning(
                "Attempt %d/%d failed for %s: %s",
                attempt, _MAX_ATTEMPTS, url, exc,
            )
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Attempt %d/%d failed for %s: %s",
                attempt, _MAX_ATTEMPTS, url, exc,
            )

        if attempt < _MAX_ATTEMPTS:
            print(f"  [RETRY] Waiting {_RETRY_WAIT}s before attempt {attempt + 1}/{_MAX_ATTEMPTS} for: {url}")
            time.sleep(_RETRY_WAIT)

    raise RuntimeError(
        f"SE-02: All {_MAX_ATTEMPTS} attempts failed for {url}. Last error: {last_exc}"
    ) from last_exc


def classify_articles(articles: List[Dict]) -> List[Dict]:
    """Classify each article via LLM. Returns ALL articles that receive a valid response.

    HIGH, MEDIUM, and LOW articles are all stored — nothing is discarded by importance.
    Only hard failures (auth error, all 5 retries exhausted) exclude an article.
    SE-01 (auth failure) aborts the entire batch immediately.

    Each returned dict contains all original fields plus:
        topic_category, relevant_persons (list of str), importance, one_line_summary
    """
    system_prompt = _load_system_prompt()
    classified: List[Dict] = []
    high_count = 0
    medium_count = 0
    low_count = 0
    error_count = 0
    total = len(articles)

    print(f"\n{'='*60}")
    print(f"BR_02 CLASSIFICATION STARTING — {total} articles to process")
    print(f"Inter-article delay: {_INTER_ARTICLE_DELAY}s | Retry wait: {_RETRY_WAIT}s | Max attempts: {_MAX_ATTEMPTS}")
    print(f"{'='*60}")

    for i, article in enumerate(articles):
        if i > 0:
            time.sleep(_INTER_ARTICLE_DELAY)

        url = article.get("url", f"article_{i}")
        title = article.get("title", "(no title)")
        user_message = _format_user_message(article)

        print(f"\n[{i+1}/{total}] Classifying: {title[:80]}")

        try:
            result = _call_with_retry(system_prompt, user_message, url)
        except RuntimeError as exc:
            error_msg = str(exc)
            if "SE-01" in error_msg:
                print(f"  [FATAL] API auth failure — aborting entire batch")
                logger.error("SE-01: API auth failure — aborting entire classification batch: %s", exc)
                raise
            print(f"  [ERROR] All {_MAX_ATTEMPTS} attempts failed — skipping article")
            logger.error("SE-02: %s", exc)
            error_count += 1
            continue
        except Exception as exc:
            print(f"  [ERROR] Unexpected failure — skipping article: {exc}")
            logger.error("BE-01: Failed to parse LLM response for %s: %s", url, exc)
            error_count += 1
            continue

        # Validate required keys (BE-01)
        missing_keys = _REQUIRED_KEYS - set(result.keys())
        if missing_keys:
            print(f"  [ERROR] Missing JSON keys {missing_keys} — skipping article")
            logger.error(
                "BE-01: LLM response for %s missing required keys %s. Raw: %s",
                url, missing_keys, result,
            )
            error_count += 1
            continue

        # Validate + fix enum values (BE-02)
        # SKIP returned by LLM is remapped to LOW — article is still stored
        result = _validate_and_fix(result, url)
        if result["importance"] == "SKIP":
            result["importance"] = "LOW"
            logger.debug("Remapped SKIP → LOW for %s (article will be stored)", url)

        # Build enriched article — ALL importance levels stored, nothing discarded
        enriched = dict(article)
        enriched["topic_category"] = result["topic_category"]
        enriched["relevant_persons"] = result["relevant_persons"]  # list of str from LLM JSON
        enriched["importance"] = result["importance"]
        enriched["one_line_summary"] = str(result.get("one_line_summary", ""))[:200]
        classified.append(enriched)

        persons_str = ", ".join(result["relevant_persons"]) if result["relevant_persons"] else "(none)"
        print(f"  [CLASSIFIED] {result['importance']} | {result['topic_category']} | {persons_str}")
        print(f"  CLASSIFIED: {title[:70]} → {result['importance']} → {persons_str}")

        if result["importance"] == "HIGH":
            high_count += 1
        elif result["importance"] == "MEDIUM":
            medium_count += 1
        else:
            low_count += 1

    # Final summary
    print(f"\n{'='*60}")
    print(f"BR_02 CLASSIFICATION COMPLETE")
    print(f"  Total articles processed : {total}")
    print(f"  Classified HIGH          : {high_count}")
    print(f"  Classified MEDIUM        : {medium_count}")
    print(f"  Classified LOW           : {low_count}")
    print(f"  Failed classification    : {error_count}")
    print(f"  → Proceeding to store    : {len(classified)} articles")
    print(f"{'='*60}\n")

    logger.info(
        "Classification complete: %d HIGH, %d MEDIUM, %d LOW, %d error(s) (of %d total).",
        high_count, medium_count, low_count, error_count, total,
    )
    return classified
