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
