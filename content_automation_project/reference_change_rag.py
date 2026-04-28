"""
Reference Change RAG Module

Extracts text (OCR) from two reference PDFs, compares them via RAG/LLM context,
and returns a structured list of changes: added, removed, merged, split, unchanged.

When use_rag=True (default for long texts): chunks both documents, embeds with
sentence-transformers, retrieves for each new chunk the k nearest old chunks,
calls LLM per (old, new) pair, then merges and deduplicates results.

Usage from code:
  from reference_change_rag import get_reference_changes, get_reference_changes_with_client
  from deepseek_api_client import DeepSeekAPIClient
  client = DeepSeekAPIClient()
  client.initialize_text_client()
  changes = get_reference_changes_with_client("old.pdf", "new.pdf", client)
  # changes = {"added": [...], "removed": [...], "merged": [...], "split": [...], "unchanged": [...]}

Usage from CLI:
  python reference_change_rag.py old.pdf new.pdf [output.json] [--gemini]
"""

import json
import logging
import re
from typing import Optional, Dict, List, Any, Callable, Tuple

from pdf_processor import PDFProcessor
from prompt_manager import PromptManager


# Max characters per document in a single request (avoids token overflow)
DEFAULT_MAX_CHARS_PER_DOC = 120000

# RAG: chunk size and overlap (characters)
RAG_CHUNK_SIZE = 4000
RAG_CHUNK_OVERLAP = 400
# Min total chars to use RAG; below that use single LLM call
RAG_MIN_TOTAL_CHARS = 25000
# Number of old chunks to retrieve per new chunk
RAG_TOP_K_OLD = 2


def _truncate_for_context(text: str, max_chars: int) -> str:
    """Keep text within max_chars; if truncated, append a truncation marker."""
    if not text or len(text) <= max_chars:
        return text or ""
    return text[: max_chars - 80].rstrip() + "\n\n[... text truncated due to length limit ...]"


def _chunk_text(text: str, chunk_size: int = RAG_CHUNK_SIZE, overlap: int = RAG_CHUNK_OVERLAP) -> List[str]:
    """Split text into overlapping chunks (by character)."""
    if not text or len(text) <= chunk_size:
        return [text] if text else []
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        if chunk.strip():
            chunks.append(chunk.strip())
        start = end - overlap
        if start >= len(text):
            break
    return chunks if chunks else [text]


def _get_embedder():
    """Lazy-load sentence-transformers model on CPU. Returns (encode_fn, True) or (None, False) if not available."""
    try:
        from sentence_transformers import SentenceTransformer
        # Force CPU to avoid CUDA errors on unsupported GPUs (e.g. GeForce 920MX / sm_50)
        model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2", device="cpu")
        def encode(texts: List[str]):
            return model.encode(texts, normalize_embeddings=True)
        return encode, True
    except Exception as e:
        logging.getLogger(__name__).warning("sentence-transformers not available for RAG: %s", e)
        return None, False


def _retrieve_k_nearest(
    query_embedding: Any,
    old_embeddings: List[Any],
    k: int = RAG_TOP_K_OLD,
) -> List[int]:
    """Return indices of k nearest old chunks (by cosine similarity; embeddings assumed normalized)."""
    if not old_embeddings or k <= 0:
        return []
    try:
        import numpy as np
        q = query_embedding if hasattr(query_embedding, "__len__") else list(query_embedding)
        scores = [float(np.dot(q, e)) for e in old_embeddings]
        indexed = list(enumerate(scores))
        indexed.sort(key=lambda x: -x[1])
        return [idx for idx, _ in indexed[:k]]
    except Exception:
        return list(range(min(k, len(old_embeddings))))


def _merge_change_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge multiple change JSONs into one; deduplicate by content key where possible."""
    merged: Dict[str, List[Any]] = {
        "added": [],
        "removed": [],
        "merged": [],
        "split": [],
        "unchanged": [],
    }
    seen_added = set()
    seen_removed = set()
    seen_merged = set()
    seen_split = set()

    def norm(s: str) -> str:
        return (s or "").strip().lower()[:200]

    for r in results:
        if not isinstance(r, dict):
            continue
        for item in r.get("added") or []:
            key = norm(item.get("title") or "") + "|" + norm(item.get("chapter") or "") if isinstance(item, dict) else str(item)
            if key not in seen_added:
                seen_added.add(key)
                merged["added"].append(item)
        for item in r.get("removed") or []:
            key = norm(item.get("title") or "") + "|" + norm(item.get("chapter") or "") if isinstance(item, dict) else str(item)
            if key not in seen_removed:
                seen_removed.add(key)
                merged["removed"].append(item)
        for item in r.get("merged") or []:
            ot = tuple(sorted((item.get("old_titles") or []) if isinstance(item, dict) else []))
            nt = norm(item.get("new_title") or "") if isinstance(item, dict) else str(item)
            key = (ot, nt)
            if key not in seen_merged:
                seen_merged.add(key)
                merged["merged"].append(item)
        for item in r.get("split") or []:
            ot = norm(item.get("old_title") or "") if isinstance(item, dict) else str(item)
            nts = tuple(sorted((item.get("new_titles") or []) if isinstance(item, dict) else []))
            key = (ot, nts)
            if key not in seen_split:
                seen_split.add(key)
                merged["split"].append(item)
        for item in r.get("unchanged") or []:
            merged["unchanged"].append(item)
    return merged


def _run_rag_pipeline(
    old_text: str,
    new_text: str,
    llm_process_text: Callable[[str, Optional[str]], Optional[str]],
    template: str,
    logger: logging.Logger,
    chunk_size: int = RAG_CHUNK_SIZE,
    overlap: int = RAG_CHUNK_OVERLAP,
    top_k: int = RAG_TOP_K_OLD,
) -> Optional[Dict[str, Any]]:
    """
    Chunk old/new texts, embed, retrieve k nearest old per new chunk, call LLM per pair, merge results.
    """
    old_chunks = _chunk_text(old_text, chunk_size, overlap)
    new_chunks = _chunk_text(new_text, chunk_size, overlap)
    if not old_chunks or not new_chunks:
        return None

    encode_fn, available = _get_embedder()
    if not available or encode_fn is None:
        logger.warning("RAG embedding not available; returning None so caller can fall back to single-call with truncated text.")
        return None

    # Embed all chunks
    old_embeddings = list(encode_fn(old_chunks))
    new_embeddings = list(encode_fn(new_chunks))
    assert len(old_embeddings) == len(old_chunks) and len(new_embeddings) == len(new_chunks)

    results = []
    for i, new_chunk in enumerate(new_chunks):
        nearest_idx = _retrieve_k_nearest(new_embeddings[i], old_embeddings, k=top_k)
        old_context = "\n\n---\n\n".join(old_chunks[j] for j in nearest_idx)
        user_text = template.replace("{OLD_TEXT}", old_context).replace("{NEW_TEXT}", new_chunk)
        response = llm_process_text(user_text, None)
        if not response:
            continue
        out = _extract_json_from_response(response)
        if out:
            results.append(out)
    if not results:
        return None
    return _merge_change_results(results)


def _extract_json_from_response(response_text: str) -> Optional[Dict[str, Any]]:
    """Extract a single JSON object from model response (with or without markdown/code block)."""
    if not response_text or not response_text.strip():
        return None
    cleaned = response_text.strip()
    # Remove markdown code block
    code_block = re.search(r"```(?:json)?\s*([\s\S]*?)```", cleaned)
    if code_block:
        cleaned = code_block.group(1).strip()
    # Find first { to last }
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start != -1 and end != -1 and end > start:
        cleaned = cleaned[start : end + 1]
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    return None


def get_reference_changes(
    pdf_old_path: str,
    pdf_new_path: str,
    llm_process_text: Callable[[str, Optional[str]], Optional[str]],
    prompt_manager: Optional[PromptManager] = None,
    prompt_name: str = "Reference Change List Prompt",
    prompt_template_override: Optional[str] = None,
    max_chars_per_doc: int = DEFAULT_MAX_CHARS_PER_DOC,
    use_pdf_processor: bool = True,
    old_text_override: Optional[str] = None,
    new_text_override: Optional[str] = None,
    use_rag: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Returns a structured list of reference changes using OCR/text of two references and an LLM.

    When use_rag=True and total text length >= RAG_MIN_TOTAL_CHARS, runs a RAG pipeline:
    chunk both texts, embed with sentence-transformers, retrieve k nearest old chunks per new chunk,
    call LLM per (old, new) pair, then merge and deduplicate results. Otherwise uses a single LLM call.

    Args:
        pdf_old_path: Path to old reference PDF (ignored if old_text_override is set).
        pdf_new_path: Path to new reference PDF (ignored if new_text_override is set).
        llm_process_text: Callable (user_text, system_prompt) -> response_text.
        prompt_manager: Prompt manager; if None, uses PromptManager().
        prompt_name: Prompt key in prompts file (used when prompt_template_override is None).
        prompt_template_override: If set, use this string as prompt template (must contain {OLD_TEXT}, {NEW_TEXT}).
        max_chars_per_doc: Max characters per document in the model input (single-call mode).
        use_pdf_processor: If True, use PDFProcessor to extract text from PDFs when overrides not set.
        old_text_override: If set, use this text instead of extracting from old PDF (e.g. from OCR file).
        new_text_override: If set, use this text instead of extracting from new PDF (e.g. from OCR file).
        use_rag: If True and texts are long enough, use RAG (chunk + embed + retrieve + LLM per pair + merge).

    Returns:
        Dict with keys: added, removed, merged, split, unchanged; or None on error.
    """
    logger = logging.getLogger(__name__)

    # 1) Old and new reference text
    old_text = old_text_override
    new_text = new_text_override

    if old_text is None or new_text is None:
        if not use_pdf_processor:
            logger.error("Without PDF processor, old_text_override and new_text_override must be provided")
            return None
        processor = PDFProcessor()
        if old_text is None:
            old_text = processor.extract_text(pdf_old_path)
            if not old_text:
                logger.error("Could not extract text from old PDF: %s", pdf_old_path)
                return None
        if new_text is None:
            new_text = processor.extract_text(pdf_new_path)
            if not new_text:
                logger.error("Could not extract text from new PDF: %s", pdf_new_path)
                return None

    total_chars = len(old_text or "") + len(new_text or "")
    if use_rag and total_chars >= RAG_MIN_TOTAL_CHARS:
        # RAG pipeline: no truncation; chunk and process per pair
        if prompt_template_override and prompt_template_override.strip():
            template = prompt_template_override.strip()
        else:
            pm = prompt_manager or PromptManager()
            template = pm.get_prompt(prompt_name)
            if not template:
                logger.error("Prompt not found: %s", prompt_name)
                return None
        out = _run_rag_pipeline(old_text, new_text, llm_process_text, template, logger)
        if out is None:
            logger.warning("RAG pipeline returned no result; falling back to single-call with truncated text")
            old_text = _truncate_for_context(old_text, max_chars_per_doc)
            new_text = _truncate_for_context(new_text, max_chars_per_doc)
            user_text = template.replace("{OLD_TEXT}", old_text).replace("{NEW_TEXT}", new_text)
            response = llm_process_text(user_text, None)
            if not response:
                return None
            out = _extract_json_from_response(response)
        if out:
            for key in ("added", "removed", "merged", "split", "unchanged"):
                if key not in out:
                    out[key] = []
            return out
        # RAG returned None: fall back to single-call with truncated text
        old_text = _truncate_for_context(old_text, max_chars_per_doc)
        new_text = _truncate_for_context(new_text, max_chars_per_doc)

    # Single-call path (short texts, use_rag=False, or RAG fallback)
    old_text = _truncate_for_context(old_text, max_chars_per_doc)
    new_text = _truncate_for_context(new_text, max_chars_per_doc)

    if prompt_template_override and prompt_template_override.strip():
        template = prompt_template_override.strip()
    else:
        pm = prompt_manager or PromptManager()
        template = pm.get_prompt(prompt_name)
        if not template:
            logger.error("Prompt not found: %s", prompt_name)
            return None
    user_text = template.replace("{OLD_TEXT}", old_text).replace("{NEW_TEXT}", new_text)

    response = llm_process_text(user_text, None)
    if not response:
        logger.error("LLM returned empty response")
        return None

    out = _extract_json_from_response(response)
    if not out:
        logger.warning("Could not parse JSON from LLM response; raw response length: %s", len(response))
        return None

    for key in ("added", "removed", "merged", "split", "unchanged"):
        if key not in out:
            out[key] = []
    return out


def get_reference_changes_with_client(
    pdf_old_path: str,
    pdf_new_path: str,
    text_client: Any,
    prompt_manager: Optional[PromptManager] = None,
    prompt_name: str = "Reference Change List Prompt",
    prompt_template_override: Optional[str] = None,
    max_chars_per_doc: int = DEFAULT_MAX_CHARS_PER_DOC,
    use_rag: bool = True,
    **kwargs: Any,
) -> Optional[Dict[str, Any]]:
    """
    Same as get_reference_changes but accepts a text client (DeepSeek or Gemini)
    that has process_text(text, system_prompt=None) instead of a callable.
    """
    def llm_process(text: str, system_prompt: Optional[str]) -> Optional[str]:
        return text_client.process_text(text, system_prompt=system_prompt)
    return get_reference_changes(
        pdf_old_path,
        pdf_new_path,
        llm_process,
        prompt_manager=prompt_manager,
        prompt_name=prompt_name,
        prompt_template_override=prompt_template_override,
        max_chars_per_doc=max_chars_per_doc,
        use_rag=use_rag,
        **kwargs,
    )


def run_from_cli(
    pdf_old: str,
    pdf_new: str,
    use_deepseek: bool = True,
    output_json_path: Optional[str] = None,
) -> None:
    """
    CLI entry: two PDF paths and optional output JSON path.
    Uses DeepSeek or Gemini and prints or saves the result.
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    if use_deepseek:
        try:
            from deepseek_api_client import DeepSeekAPIClient
            from api_layer import APIKeyManager
            client = DeepSeekAPIClient(api_key_manager=APIKeyManager())
            client.initialize_text_client()
            def llm_process(text: str, system_prompt: Optional[str]) -> Optional[str]:
                return client.process_text(text, system_prompt=system_prompt)
        except Exception as e:
            logger.error("DeepSeek not available: %s", e)
            return
    else:
        try:
            from api_layer import GeminiAPIClient
            client = GeminiAPIClient()
            if not client.initialize_text_client():
                logger.error("Gemini text client init failed")
                return
            def llm_process(text: str, system_prompt: Optional[str]) -> Optional[str]:
                return client.process_text(text, system_prompt=system_prompt)
        except Exception as e:
            logger.error("Gemini not available: %s", e)
            return

    result = get_reference_changes(pdf_old, pdf_new, llm_process)
    if result is None:
        logger.error("get_reference_changes failed")
        return
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if output_json_path:
        with open(output_json_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("Saved to %s", output_json_path)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python reference_change_rag.py <pdf_old> <pdf_new> [output.json] [--gemini]")
        sys.exit(1)
    pdf_old = sys.argv[1]
    pdf_new = sys.argv[2]
    out_path = None
    use_deepseek = True
    for i in range(3, len(sys.argv)):
        if sys.argv[i] == "--gemini":
            use_deepseek = False
        elif not sys.argv[i].startswith("-"):
            out_path = sys.argv[i]
    run_from_cli(pdf_old, pdf_new, use_deepseek=use_deepseek, output_json_path=out_path)
