"""L1 — Input sanitization via regex pattern matching."""

from __future__ import annotations

import re


class PromptInjectionError(Exception):
    """Raised when input matches a known prompt injection pattern."""


PROMPT_INJECTION_PATTERNS = [
    r"ignore (previous|above|all) instructions",
    r"system prompt",
    r"you are now",
    r"act as",
    r"disregard",
    r"forget everything",
    r"new instructions",
    r"<\|.*?\|>",
    r"\[INST\]",
    r"###\s*(instruction|system|human|assistant)",
]

_COMPILED_PATTERNS = [re.compile(p, re.IGNORECASE) for p in PROMPT_INJECTION_PATTERNS]


def sanitize_for_prompt(value: str) -> str:
    """Sanitize user input for use in LLM prompts.

    Raises PromptInjectionError if a known injection pattern is detected.
    Strips control characters and truncates to 500 chars.
    """
    for pattern in _COMPILED_PATTERNS:
        if pattern.search(value):
            raise PromptInjectionError(f"Blocked: {value[:50]}")

    # Strip control characters
    value = re.sub(r"[\x00-\x1f\x7f]", "", value)

    # Truncate to max length
    return value[:500]
