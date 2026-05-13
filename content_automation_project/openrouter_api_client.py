"""
OpenRouter API Client for Content Automation Project
Routes text/PDF requests to OpenRouter chat completions endpoint.

Docs: https://openrouter.ai/docs/quickstart
"""

from __future__ import annotations

import json
import logging
import os
from typing import Optional, Dict, Any, Callable, List

import requests

from api_layer import APIKeyManager, APIConfig


class OpenRouterRequestAborted(Exception):
    """Raised when cancel_check() is true during a streamed request (user stop)."""


class OpenRouterAPIError(Exception):
    """
    Raised when OpenRouter returns a failing HTTP status or an error payload (e.g. context length exceeded).
    Carries the parsed provider message so UIs and logs can show it verbatim.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        api_message: Optional[str] = None,
        model_name: Optional[str] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.api_message = api_message
        self.model_name = model_name


def _rough_token_estimate_from_chars(char_count: int) -> int:
    """Very rough token estimate (~4 chars/token for Latin/mixed text)."""
    return max(1, char_count // 4)


def _parse_openrouter_error_body(resp: requests.Response) -> str:
    """Extract human-readable error text from OpenRouter error JSON or raw body."""
    text = (resp.text or "").strip()
    if not text:
        return resp.reason or f"HTTP {resp.status_code}"
    try:
        data = resp.json()
    except json.JSONDecodeError:
        return text[:4000]
    err = data.get("error")
    if isinstance(err, dict):
        return (err.get("message") or err.get("type") or json.dumps(err, ensure_ascii=False))[:4000]
    if isinstance(err, str):
        return err[:4000]
    return (data.get("message") or text)[:4000]


def _build_openrouter_error_message(*, resp: requests.Response) -> tuple[str, str]:
    """Return (exception_message, parsed_provider_message). Provider text only in the user-facing line."""
    api_msg = _parse_openrouter_error_body(resp)
    full = f"OpenRouter HTTP {resp.status_code}: {api_msg}"
    return full, api_msg


def _log_openrouter_request_context(
    logger: logging.Logger,
    *,
    model_name: str,
    prompt_char_len: int,
    max_tokens: int,
) -> None:
    """Diagnostics for operators only — not appended to OpenRouterAPIError."""
    logger.info(
        "OpenRouter request context: model=%s max_tokens=%s prompt_chars=%s (~%s tokens rough)",
        model_name,
        max_tokens,
        prompt_char_len,
        _rough_token_estimate_from_chars(prompt_char_len),
    )


class OpenRouterAPIClient:
    """API client for OpenRouter (chat/completions)."""

    def __init__(self, api_key_manager: Optional[APIKeyManager] = None):
        self.key_manager = api_key_manager or APIKeyManager()
        self.logger = logging.getLogger(__name__)
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"
        self._current_model_name: Optional[str] = None

        # Session with retries (similar to DeepSeek client)
        self.session = requests.Session()
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)

    def _resolve_api_key(self, explicit_api_key: Optional[str] = None) -> Optional[str]:
        """Resolve API key from explicit parameter or OPENROUTER_API_KEY env."""
        if explicit_api_key and explicit_api_key.strip():
            return explicit_api_key.strip()
        key = self.key_manager.get_next_key()
        if key and key.strip():
            return key.strip()
        return (os.getenv("OPENROUTER_API_KEY") or "").strip() or None

    def extract_from_code_block(self, text: str) -> str:
        """Compatibility helper shared by processors."""
        if not text:
            return text
        text_stripped = text.strip()
        if text_stripped.startswith("```"):
            first_newline = text_stripped.find("\n")
            if first_newline == -1:
                return text_stripped.strip("`").strip()
            end_marker = text_stripped.rfind("```")
            if end_marker > first_newline:
                return text_stripped[first_newline + 1:end_marker].strip()
        return text

    def initialize_text_client(self, model_name: str = APIConfig.DEFAULT_OPENROUTER_MODEL,
                               api_key: Optional[str] = None) -> bool:
        """Compatibility initializer (stores model; validates API key availability)."""
        key = self._resolve_api_key(api_key)
        if not key:
            self.logger.error("No OpenRouter API key available")
            return False
        self._current_model_name = model_name
        self.logger.info(f"OpenRouter client initialized with model: {model_name}")
        return True

    def _stream_chat_completions(
        self,
        *,
        model_name: str,
        user_text: str,
        system_prompt: Optional[str],
        temperature: float,
        max_tokens: int,
        api_key: Optional[str],
        timeout_s: float,
        cancel_check: Callable[[], bool],
    ) -> Optional[str]:
        """Stream tokens from OpenRouter and check cancel between SSE lines (enables user stop)."""
        key = self._resolve_api_key(api_key)
        if not key:
            self.logger.error("No OpenRouter API key available")
            return None

        messages: List[Dict[str, str]] = []
        if system_prompt and system_prompt.strip():
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_text})

        prompt_char_len = len(user_text) + len(system_prompt or "")

        headers = {
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://content-automation.local",
            "X-Title": "Content Automation",
        }
        payload: Dict[str, Any] = {
            "model": model_name,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": True,
        }

        if cancel_check():
            raise OpenRouterRequestAborted()
        try:
            resp = self.session.post(
                self.base_url, headers=headers, json=payload, stream=True, timeout=timeout_s
            )
            if resp.status_code >= 400:
                _log_openrouter_request_context(
                    self.logger,
                    model_name=model_name,
                    prompt_char_len=prompt_char_len,
                    max_tokens=max_tokens,
                )
                full, api_msg = _build_openrouter_error_message(resp=resp)
                self.logger.error(full)
                raise OpenRouterAPIError(
                    full,
                    status_code=resp.status_code,
                    api_message=api_msg,
                    model_name=model_name,
                )
            resp.raise_for_status()
            # text/event-stream often has no charset; requests defaults to ISO-8859-1 and mojibakes UTF-8
            # (e.g. Persian) before json.loads. Force UTF-8 for iter_lines decode.
            resp.encoding = "utf-8"
            chunks: List[str] = []
            for raw in resp.iter_lines(decode_unicode=True):
                if cancel_check():
                    resp.close()
                    raise OpenRouterRequestAborted()
                if not raw:
                    continue
                line = raw.strip()
                if not line.startswith("data: "):
                    continue
                data = line[6:].strip()
                if data == "[DONE]":
                    break
                try:
                    obj = json.loads(data)
                except json.JSONDecodeError:
                    continue
                err = obj.get("error")
                if err:
                    msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
                    _log_openrouter_request_context(
                        self.logger,
                        model_name=model_name,
                        prompt_char_len=prompt_char_len,
                        max_tokens=max_tokens,
                    )
                    full = f"OpenRouter stream error: {msg}"
                    self.logger.error(full)
                    raise OpenRouterAPIError(full, api_message=msg, model_name=model_name)
                for choice in obj.get("choices") or []:
                    delta = choice.get("delta") or {}
                    if not isinstance(delta, dict):
                        continue
                    piece = delta.get("content") or delta.get("reasoning")
                    if piece:
                        chunks.append(piece)
            return "".join(chunks) if chunks else None
        except OpenRouterRequestAborted:
            raise
        except OpenRouterAPIError:
            raise
        except Exception as e:
            self.logger.error("OpenRouter streaming failed: %s", e)
            return None

    def _call_chat_completions(
        self,
        *,
        model_name: str,
        user_text: str,
        system_prompt: Optional[str],
        temperature: float,
        max_tokens: int,
        api_key: Optional[str],
        timeout_s: float = 600.0,
        cancel_check: Optional[Callable[[], bool]] = None,
        use_streaming: bool = False,
    ) -> Optional[str]:
        if use_streaming:
            return self._stream_chat_completions(
                model_name=model_name,
                user_text=user_text,
                system_prompt=system_prompt,
                temperature=temperature,
                max_tokens=max_tokens,
                api_key=api_key,
                timeout_s=timeout_s,
                cancel_check=cancel_check,
            )

        key = self._resolve_api_key(api_key)
        if not key:
            self.logger.error("No OpenRouter API key available")
            return None

        messages: List[Dict[str, str]] = []
        if system_prompt and system_prompt.strip():
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_text})

        prompt_char_len = len(user_text) + len(system_prompt or "")

        headers = {
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            # Optional attribution headers (safe defaults)
            "HTTP-Referer": "https://content-automation.local",
            "X-Title": "Content Automation",
        }

        payload: Dict[str, Any] = {
            "model": model_name,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False,
        }

        try:
            resp = self.session.post(self.base_url, headers=headers, json=payload, timeout=timeout_s)
            if resp.status_code >= 400:
                _log_openrouter_request_context(
                    self.logger,
                    model_name=model_name,
                    prompt_char_len=prompt_char_len,
                    max_tokens=max_tokens,
                )
                full, api_msg = _build_openrouter_error_message(resp=resp)
                self.logger.error(full)
                raise OpenRouterAPIError(
                    full,
                    status_code=resp.status_code,
                    api_message=api_msg,
                    model_name=model_name,
                )
            resp.raise_for_status()
            data = resp.json()
            err = data.get("error")
            if err:
                api_msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
                _log_openrouter_request_context(
                    self.logger,
                    model_name=model_name,
                    prompt_char_len=prompt_char_len,
                    max_tokens=max_tokens,
                )
                full = f"OpenRouter response JSON error: {api_msg}"
                self.logger.error(full)
                raise OpenRouterAPIError(
                    full,
                    status_code=None,
                    api_message=api_msg,
                    model_name=model_name,
                )
            return (data.get("choices") or [{}])[0].get("message", {}).get("content")
        except OpenRouterAPIError:
            raise
        except Exception as e:
            self.logger.error(f"OpenRouter request failed: {e}")
            return None

    def process_text(
        self,
        text: str,
        system_prompt: Optional[str] = None,
        model_name: str = APIConfig.DEFAULT_OPENROUTER_MODEL,
        temperature: float = APIConfig.DEFAULT_TEMPERATURE,
        max_tokens: int = APIConfig.DEFAULT_MAX_TOKENS,
        api_key: Optional[str] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
        use_streaming: bool = False,
        timeout_s: float = 600.0,
    ) -> Optional[str]:
        """Process text via OpenRouter chat completions.

        Non-streaming is the default for higher stability. Set use_streaming=True to enable
        streamed responses (e.g. when frequent cancel checks during generation are required).
        """
        if not model_name:
            model_name = APIConfig.DEFAULT_OPENROUTER_MODEL
        self._current_model_name = model_name
        return self._call_chat_completions(
            model_name=model_name,
            user_text=text,
            system_prompt=system_prompt,
            temperature=temperature,
            max_tokens=max_tokens,
            api_key=api_key,
            timeout_s=timeout_s,
            cancel_check=cancel_check,
            use_streaming=use_streaming,
        )

    def process_pdf_with_prompt(
        self,
        pdf_path: str,
        prompt: str,
        model_name: str = APIConfig.DEFAULT_OPENROUTER_MODEL,
        temperature: float = APIConfig.DEFAULT_TEMPERATURE,
        max_tokens: int = APIConfig.DEFAULT_MAX_TOKENS,
        api_key: Optional[str] = None,
        return_json: bool = False,
        force_no_streaming: bool = False,
    ) -> Optional[str]:
        """
        OpenRouter does not accept direct PDF upload in this project.
        We extract PDF text locally, then send it as user content with the prompt as system.
        """
        if not os.path.exists(pdf_path):
            self.logger.error(f"PDF file not found: {pdf_path}")
            return None

        from pdf_processor import PDFProcessor

        pdf_proc = PDFProcessor()
        extracted = pdf_proc.extract_text(pdf_path)
        if not extracted:
            self.logger.error("Failed to extract text from PDF for OpenRouter processing")
            return None

        # Keep prompt as system message, PDF text as user message.
        user_text = extracted
        system_prompt = prompt

        resp = self.process_text(
            text=user_text,
            system_prompt=system_prompt,
            model_name=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            api_key=api_key,
        )
        if not resp:
            return None
        if return_json:
            # Leave JSON extraction to existing post-processing (BaseStageProcessor) upstream.
            return resp
        return resp

    def process_pdf_with_prompt_batch(
        self,
        pdf_path: str,
        prompt: str,
        model_name: str = APIConfig.DEFAULT_OPENROUTER_MODEL,
        temperature: float = APIConfig.DEFAULT_TEMPERATURE,
        max_tokens: int = APIConfig.DEFAULT_MAX_TOKENS,
        pages_per_batch: int = 10,
        rows_per_batch: int = 500,
        api_key: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> Optional[str]:
        """
        Batch processing for OpenRouter: extract text per page-range and call process_text for each batch.
        Returns a combined TXT string (callers can save/parse it like Gemini/DeepSeek flows).
        """
        if not os.path.exists(pdf_path):
            self.logger.error(f"PDF file not found: {pdf_path}")
            return None

        from pdf_processor import PDFProcessor

        pdf_proc = PDFProcessor()
        total_pages = pdf_proc.count_pages(pdf_path)
        if total_pages <= 0:
            self.logger.error("PDF has no pages")
            return None

        if progress_callback:
            progress_callback(f"[OpenRouter] PDF has {total_pages} pages. Starting batch text extraction...")

        responses = []
        start = 1
        while start <= total_pages:
            end = min(start + max(1, pages_per_batch) - 1, total_pages)
            if progress_callback:
                progress_callback(f"[OpenRouter] Extracting pages {start}-{end}...")

            batch_text = pdf_proc.extract_text_range(pdf_path, start, end)
            if not batch_text:
                self.logger.warning(f"[OpenRouter] No text extracted for pages {start}-{end}")
                start = end + 1
                continue

            # Add page-range marker to help model align.
            user_text = f"Pages {start}-{end}:\n{batch_text}"
            system_prompt = prompt

            if progress_callback:
                progress_callback(f"[OpenRouter] Calling model for pages {start}-{end}...")

            out = self.process_text(
                text=user_text,
                system_prompt=system_prompt,
                model_name=model_name,
                temperature=temperature,
                max_tokens=max_tokens,
                api_key=api_key,
            )
            if out:
                responses.append(out)
            start = end + 1

        if not responses:
            return None
        return "\n\n".join(responses)

