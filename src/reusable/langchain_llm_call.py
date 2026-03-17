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

import json
import logging
import os
import re
import time
from typing import Any, Union

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from openai import AuthenticationError, RateLimitError, APIStatusError

load_dotenv()

logger = logging.getLogger(__name__)

_RETRY_DELAYS = [2, 4, 8, 16, 32]


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, RateLimitError):
        return True
    if isinstance(exc, APIStatusError) and exc.status_code >= 500:
        return True
    return False


def _strip_markdown_json(text: str) -> str:
    """Remove ```json ... ``` or ``` ... ``` fences, then strip whitespace."""
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


def llm_call(
    system_prompt: str,
    user_message: str,
    model_name: str = "gpt-4o-mini",
    temperature: float = 0.0,
    max_tokens: int = 4096,
    expect_json: bool = False,
) -> Union[str, Any]:
    """Call ChatOpenAI and return the response content.

    Returns parsed JSON (dict/list) when expect_json=True, otherwise raw str.

    Raises:
        SE-01: AuthenticationError for invalid API key / billing issues.
        SE-02: RateLimitError or server 5xx after 5 retries exhausted.
    """
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("SE-01: OPENAI_API_KEY environment variable is not set.")

    llm = ChatOpenAI(
        model=model_name,
        temperature=temperature,
        max_tokens=max_tokens,
        api_key=api_key,
    )

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_message),
    ]

    last_exc = None
    for attempt, delay in enumerate([0] + _RETRY_DELAYS):
        if delay:
            logger.warning(
                "SE-02: Retryable error, backing off %ds (attempt %d/%d): %s",
                delay,
                attempt,
                len(_RETRY_DELAYS),
                last_exc,
            )
            time.sleep(delay)
        try:
            response = llm.invoke(messages)
            break
        except AuthenticationError as exc:
            raise RuntimeError(f"SE-01: Invalid API key or billing issue: {exc}") from exc
        except Exception as exc:
            if _is_retryable(exc):
                last_exc = exc
            else:
                raise
    else:
        raise RuntimeError(f"SE-02: Max retries exceeded. Last error: {last_exc}") from last_exc

    # Log token usage
    usage = getattr(response, "usage_metadata", None) or getattr(response, "response_metadata", {}).get("token_usage", {})
    if usage:
        prompt_tokens = getattr(usage, "input_tokens", None) or usage.get("prompt_tokens", "?")
        completion_tokens = getattr(usage, "output_tokens", None) or usage.get("completion_tokens", "?")
        total_tokens = getattr(usage, "total_tokens", None) or usage.get("total_tokens", "?")
        logger.info(
            "Token usage — prompt: %s, completion: %s, total: %s (model=%s)",
            prompt_tokens,
            completion_tokens,
            total_tokens,
            model_name,
        )
    else:
        logger.debug("Token usage metadata not available in response.")

    content = response.content

    if expect_json:
        cleaned = _strip_markdown_json(content)
        return json.loads(cleaned)

    return content
