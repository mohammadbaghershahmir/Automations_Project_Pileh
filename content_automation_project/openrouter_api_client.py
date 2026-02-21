"""
OpenRouter API Client for Content Automation Project
Routes text/PDF requests to OpenRouter chat completions endpoint.

Docs: https://openrouter.ai/docs/quickstart
"""

from __future__ import annotations

import json
import logging
import os
from typing import Optional, Dict, Any, Callable

import requests

from api_layer import APIKeyManager, APIConfig


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

    def initialize_text_client(self, model_name: str = APIConfig.DEFAULT_OPENROUTER_MODEL,
                               api_key: Optional[str] = None) -> bool:
        """Compatibility initializer (stores model; validates API key availability)."""
        key = api_key or self.key_manager.get_next_key()
        if not key:
            self.logger.error("No OpenRouter API key available")
            return False
        self._current_model_name = model_name
        self.logger.info(f"OpenRouter client initialized with model: {model_name}")
        return True

    def _call_chat_completions(
        self,
        *,
        model_name: str,
        user_text: str,
        system_prompt: Optional[str],
        temperature: float,
        max_tokens: int,
        api_key: Optional[str],
        timeout_s: float = 300.0,
    ) -> Optional[str]:
        key = api_key or self.key_manager.get_next_key()
        if not key:
            self.logger.error("No OpenRouter API key available")
            return None

        messages = []
        if system_prompt and system_prompt.strip():
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_text})

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
            resp.raise_for_status()
            data = resp.json()
            return (data.get("choices") or [{}])[0].get("message", {}).get("content")
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
    ) -> Optional[str]:
        """Process text via OpenRouter chat completions."""
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

