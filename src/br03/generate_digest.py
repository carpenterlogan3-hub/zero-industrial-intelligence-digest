"""BR_03_generate_digest.py — V2

For each person with articles assigned to them:
    1. Look up their prompt template from config/distribution_config.yaml (prompt_template_file field).
    2. Load the template from disk.
    3. Call REUSABLE_langchain_llm_call: model=gpt-4o-mini, temperature=0.4, max_tokens=1500.
    4. Validate response >= 100 characters.

V2 CHANGES:
- Iterates over person names (not role keys)
- Matches person name to distribution_config.yaml stakeholder entries to find their prompt template
- 9 possible team members, each with their own tailored prompt

Prompt templates in config/prompts/:
    digest_ceo.txt, digest_svp_dev_canada.txt, digest_vp_finance.txt,
    digest_bd.txt, digest_bd_analyst.txt, digest_ea.txt,
    digest_investor_rcm.txt, digest_investor_evok.txt, digest_decarb.txt

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


def _build_name_to_stakeholder(stakeholders: List[Dict]) -> Dict[str, Dict]:
    """Build a case-sensitive lookup dict from person name → stakeholder config."""
    return {s["name"]: s for s in stakeholders}


def _format_articles_for_prompt(articles: List[Dict]) -> str:
    """Format articles as a structured list for the LLM user message."""
    lines = [f"Today is {_today_display()}. Here are today's classified news items:\n"]
    importance_order = {"HIGH": 0, "MEDIUM": 1}
    sorted_articles = sorted(
        articles,
        key=lambda a: importance_order.get(a.get("importance", "MEDIUM"), 1),
    )
    for i, article in enumerate(sorted_articles, 1):
        lines.append(
            f"{i}. [{article.get('importance', '')}] {article.get('title', '(no title)')}\n"
            f"   Source: {article.get('source', '')} | Category: {article.get('topic_category', '')}\n"
            f"   URL: {article.get('url', '')}\n"
            f"   Summary: {article.get('one_line_summary', article.get('summary', ''))}"
        )
    return "\n\n".join(lines)


def _generate_html_fallback(person_name: str, articles: List[Dict], reason: str) -> str:
    """Generate a plain HTML bullet-list digest when LLM generation fails."""
    date_str = _today_display()
    importance_order = {"HIGH": 0, "MEDIUM": 1}
    sorted_articles = sorted(
        articles,
        key=lambda a: importance_order.get(a.get("importance", "MEDIUM"), 1),
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
        f"<p>For: <strong>{person_name}</strong></p>"
        f"<p><em>Note: Automated digest generation encountered an issue ({reason}). "
        f"Displaying raw classified items below.</em></p>"
        f"<ul>\n{items_html}</ul>"
        f"</body></html>"
    )


def generate_digests(
    stakeholders: List[Dict],
    articles_by_person: Dict[str, List[Dict]],
) -> List[Dict]:
    """Generate an HTML digest for each person who has articles today.

    Matches each person name from articles_by_person to a stakeholder entry
    in distribution_config (by 'name' field) to find their prompt template.

    People with articles but no matching stakeholder config are skipped with a warning.
    People with no articles are silently skipped.

    Returns list of dicts: {stakeholder: Dict, digest_html: str, article_count: int}.
    """
    name_to_stakeholder = _build_name_to_stakeholder(stakeholders)
    results = []

    for person_name, articles in articles_by_person.items():
        if not articles:
            continue

        stakeholder = name_to_stakeholder.get(person_name)
        if stakeholder is None:
            logger.warning(
                "No stakeholder config found for person '%s' — skipping digest. "
                "Check distribution_config.yaml for a matching 'name' entry.",
                person_name,
            )
            continue

        system_prompt = _load_template(stakeholder["prompt_template_file"])
        user_message = _format_articles_for_prompt(articles)
        digest_html: Optional[str] = None
        last_exc: Optional[Exception] = None

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
                        person_name, len(response.strip()), attempt,
                    )
                    digest_html = _generate_html_fallback(
                        person_name, articles, "response too short"
                    )
                else:
                    digest_html = response
                break
            except RuntimeError as exc:
                if "SE-01" in str(exc):
                    logger.error(
                        "SE-01: API auth failure generating digest for '%s': %s", person_name, exc
                    )
                    raise
                logger.warning(
                    "SE-01: LLM call failed for '%s' on attempt %d/%d: %s",
                    person_name, attempt, _MAX_LLM_ATTEMPTS, exc,
                )
                last_exc = exc
            except Exception as exc:
                logger.warning(
                    "SE-01: Unexpected error for '%s' on attempt %d/%d: %s",
                    person_name, attempt, _MAX_LLM_ATTEMPTS, exc,
                )
                last_exc = exc

        if digest_html is None:
            logger.error(
                "SE-01: All %d attempts failed for '%s'. Generating HTML fallback. Last error: %s",
                _MAX_LLM_ATTEMPTS, person_name, last_exc,
            )
            digest_html = _generate_html_fallback(
                person_name, articles,
                f"LLM unavailable after {_MAX_LLM_ATTEMPTS} attempts",
            )

        results.append({
            "stakeholder": stakeholder,
            "digest_html": digest_html,
            "article_count": len(articles),
        })
        logger.info(
            "Generated digest for '%s' — %d article(s), %d chars.",
            person_name, len(articles), len(digest_html),
        )

    return results
