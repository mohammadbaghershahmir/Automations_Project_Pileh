"""
OpenRouter model registry — single source of truth for job model dropdowns and API routing.

DeepSeek V4 Flash supports OpenRouter reasoning.effort "high" and "xhigh" (xhigh = max reasoning).
See: https://openrouter.ai/deepseek/deepseek-v4-flash/api
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

DEEPSEEK_V4_FLASH = "deepseek/deepseek-v4-flash"
DEEPSEEK_V4_FLASH_REASONING_HIGH = f"{DEEPSEEK_V4_FLASH} (reasoning high)"
DEEPSEEK_V4_FLASH_REASONING_XHIGH = f"{DEEPSEEK_V4_FLASH} (reasoning xhigh)"

GEMINI_35_FLASH = "google/gemini-3.5-flash"
GLM_5 = "z-ai/glm-5"
GLM_51 = "z-ai/glm-5.1"


@dataclass(frozen=True)
class OpenRouterModelOption:
    """Selectable model in job UIs; may map to an API slug plus optional reasoning config."""

    choice_id: str
    api_model: str
    reasoning: Optional[Dict[str, Any]] = None


OPENROUTER_MODEL_OPTIONS: tuple[OpenRouterModelOption, ...] = (
    OpenRouterModelOption(GLM_5, GLM_5),
    OpenRouterModelOption(GLM_51, GLM_51),
    OpenRouterModelOption(GEMINI_35_FLASH, GEMINI_35_FLASH),
    OpenRouterModelOption(DEEPSEEK_V4_FLASH, DEEPSEEK_V4_FLASH),
    OpenRouterModelOption(
        DEEPSEEK_V4_FLASH_REASONING_HIGH,
        DEEPSEEK_V4_FLASH,
        reasoning={"effort": "high"},
    ),
    OpenRouterModelOption(
        DEEPSEEK_V4_FLASH_REASONING_XHIGH,
        DEEPSEEK_V4_FLASH,
        reasoning={"effort": "xhigh"},
    ),
    OpenRouterModelOption("qwen/qwen3.6-plus", "qwen/qwen3.6-plus"),
    OpenRouterModelOption("qwen/qwen3.5-plus-20260420", "qwen/qwen3.5-plus-20260420"),
    OpenRouterModelOption("google/gemini-2.5-pro", "google/gemini-2.5-pro"),
    OpenRouterModelOption("opencounter/opencounter", "opencounter/opencounter"),
    OpenRouterModelOption("opencounter", "opencounter"),
)

OPENROUTER_MODEL_CHOICE_IDS: tuple[str, ...] = tuple(
    option.choice_id for option in OPENROUTER_MODEL_OPTIONS
)

_REGISTRY: Dict[str, OpenRouterModelOption] = {
    option.choice_id: option for option in OPENROUTER_MODEL_OPTIONS
}


def resolve_openrouter_model_choice(
    choice_id: str,
    *,
    default_api_model: str = GLM_5,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    Map a job/UI model choice to (OpenRouter api_model, openrouter_payload_extra).

    Unknown choice_ids pass through as raw OpenRouter slugs for backward compatibility.
    """
    normalized = (choice_id or "").strip()
    if not normalized:
        return default_api_model, None

    option = _REGISTRY.get(normalized)
    if option is None:
        return normalized, None

    if option.reasoning:
        return option.api_model, {"reasoning": dict(option.reasoning)}
    return option.api_model, None


def merge_openrouter_payload_extras(
    base: Optional[Dict[str, Any]],
    extra: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Shallow-merge two OpenRouter payload extra dicts (e.g. model reasoning + caller overrides)."""
    if not base:
        return dict(extra) if extra else None
    if not extra:
        return dict(base)

    merged = dict(base)
    for key, value in extra.items():
        if (
            key == "reasoning"
            and isinstance(value, dict)
            and isinstance(merged.get("reasoning"), dict)
        ):
            merged["reasoning"] = {**merged["reasoning"], **value}
        else:
            merged[key] = value
    return merged
