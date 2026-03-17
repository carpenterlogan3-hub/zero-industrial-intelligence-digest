"""REUSABLE_langchain_llm_call.py

Wrapper around LangChain ChatOpenAI. Parameters:
    model_name: str (default 'gpt-4o-mini')
    temperature: float
    max_tokens: int
    system_prompt: str
    user_message: str
    expect_json: bool (default False)

When expect_json=True: strips markdown backticks, parses as JSON.
Retry: exponential backoff 2s/4s/8s/16s/32s (5 retries) for 429/5xx.
Logs token usage: prompt_tokens, completion_tokens, total_tokens.

Called by: BR_02_classify_article (json, temp=0.2), BR_03_generate_digest (html, temp=0.4).

Exceptions:
    SE-01: Invalid API key / billing → raise immediately (no retry).
    SE-02: Rate limit 429 / server 5xx → backoff 5x, then raise.
"""
