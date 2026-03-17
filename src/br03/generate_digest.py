"""BR_03_generate_digest.py

For each stakeholder with articles assigned to their role:
    1. Load prompt template from prompt_template_file path.
    2. Call REUSABLE_langchain_llm_call: model=gpt-4o-mini, temperature=0.4, max_tokens=1500.
    3. Validate response >= 100 characters.

Prompt templates: config/prompts/digest_ceo.txt, digest_svp_dev_canada.txt,
digest_vp_finance.txt, digest_ai_it.txt.

Exceptions:
    SE-01: OpenAI API fail → retry 3x, then generate FALLBACK (HTML bullet list).
    BE-01: Empty/short response → generate FALLBACK (HTML bullet list).
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from src.reusable.langchain_llm_call import llm_call

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parent.parent.parent
_ET_TZ = ZoneInfo("America/New_York")
_MIN_RESPONSE_CHARS = 100
_MAX_LLM_ATTEMPTS = 3


def _today_display() -> str:
    return datetime.now(timezone.utc).astimezone(_ET_TZ).strftime("%B %d, %Y")


def _load_template(path_str: str) -> str:
    path = _PROJECT_ROOT / path_str
    return path.read_text(encoding="utf-8").strip()


def _format_articles_for_prompt(articles: List[Dict]) -> str:
    """Format articles as a structured list for the LLM user message."""
    lines = [f"Today is {_today_display()}. Here are today's classified news items:\n"]
    # Sort by importance: HIGH first, then MEDIUM, then LOW
    importance_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    sorted_articles = sorted(
        articles,
        key=lambda a: importance_order.get(a.get("importance", "LOW"), 2),
    )
    for i, article in enumerate(sorted_articles, 1):
        lines.append(
            f"{i}. [{article.get('importance', '')}] {article.get('title', '(no title)')}\n"
            f"   Source: {article.get('source', '')} | Category: {article.get('topic_category', '')}\n"
            f"   URL: {article.get('url', '')}\n"
            f"   Summary: {article.get('one_line_summary', article.get('summary', ''))}"
        )
    return "\n\n".join(lines)


def _generate_html_fallback(stakeholder: Dict, articles: List[Dict], reason: str) -> str:
    """Generate a plain HTML bullet-list digest when LLM generation fails."""
    name = stakeholder.get("name", "Stakeholder")
    date_str = _today_display()
    importance_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    sorted_articles = sorted(
        articles,
        key=lambda a: importance_order.get(a.get("importance", "LOW"), 2),
    )
    items_html = ""
    for article in sorted_articles:
        url = article.get("url", "#")
        title = article.get("title", "(no title)")
        source = article.get("source", "")
        importance = article.get("importance", "")
        summary = article.get("one_line_summary", article.get("summary", ""))
        items_html += (
            f'<li><strong><a href="{url}">{title}</a></strong> '
            f'[{importance}] — {source}<br>'
            f'<em>{summary}</em></li>\n'
        )
    return (
        f"<html><body>"
        f"<h2>Zero Industrial Daily Intel Digest — {date_str}</h2>"
        f"<p><em>Note: Automated digest generation encountered an issue ({reason}). "
        f"Displaying raw classified items below.</em></p>"
        f"<ul>\n{items_html}</ul>"
        f"</body></html>"
    )


def generate_digests(
    stakeholders: List[Dict],
    articles_by_role: Dict[str, List[Dict]],
) -> List[Dict]:
    """Generate an HTML digest for each stakeholder that has articles today.

    Returns a list of dicts: {stakeholder: Dict, digest_html: str, article_count: int}.
    Stakeholders with no articles for their role are skipped.
    """
    results = []

    for stakeholder in stakeholders:
        role_key = stakeholder["role_key"]
        name = stakeholder["name"]
        articles = articles_by_role.get(role_key, [])

        if not articles:
            logger.info("No articles for role '%s' (%s) — skipping digest.", role_key, name)
            continue

        system_prompt = _load_template(stakeholder["prompt_template_file"])
        user_message = _format_articles_for_prompt(articles)
        digest_html: Optional[str] = None
        last_exc: Optional[Exception] = None

        # SE-01: up to 3 outer attempts (each call may internally retry for rate limits)
        for attempt in range(1, _MAX_LLM_ATTEMPTS + 1):
            try:
                response = llm_call(
                    system_prompt=system_prompt,
                    user_message=user_message,
                    model_name="gpt-4o-mini",
                    temperature=0.4,
                    max_tokens=1500,
                    expect_json=False,
                )
                # BE-01: validate minimum length
                if len(response.strip()) < _MIN_RESPONSE_CHARS:
                    logger.warning(
                        "BE-01: LLM response for '%s' too short (%d chars) on attempt %d — "
                        "using HTML fallback.",
                        name, len(response.strip()), attempt,
                    )
                    digest_html = _generate_html_fallback(stakeholder, articles, "response too short")
                else:
                    digest_html = response
                break
            except RuntimeError as exc:
                error_msg = str(exc)
                if "SE-01" in error_msg:
                    # Auth failure — abort immediately, don't retry
                    logger.error("SE-01: API auth failure generating digest for '%s': %s", name, exc)
                    raise
                logger.warning(
                    "SE-01: LLM call failed for '%s' on attempt %d/%d: %s",
                    name, attempt, _MAX_LLM_ATTEMPTS, exc,
                )
                last_exc = exc
            except Exception as exc:
                logger.warning(
                    "SE-01: Unexpected error for '%s' on attempt %d/%d: %s",
                    name, attempt, _MAX_LLM_ATTEMPTS, exc,
                )
                last_exc = exc

        if digest_html is None:
            logger.error(
                "SE-01: All %d attempts failed for '%s'. Generating HTML fallback. Last error: %s",
                _MAX_LLM_ATTEMPTS, name, last_exc,
            )
            digest_html = _generate_html_fallback(
                stakeholder, articles, f"LLM unavailable after {_MAX_LLM_ATTEMPTS} attempts"
            )

        results.append({
            "stakeholder": stakeholder,
            "digest_html": digest_html,
            "article_count": len(articles),
        })
        logger.info(
            "Generated digest for '%s' (%s) — %d article(s), %d chars.",
            name, role_key, len(articles), len(digest_html),
        )

    return results
