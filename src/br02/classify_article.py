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
