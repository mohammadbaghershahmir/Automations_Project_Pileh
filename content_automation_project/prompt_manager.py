"""
Prompt Manager Module
Manages predefined prompts and custom prompt handling
"""

import json
import os
import logging
from typing import Dict, List, Optional
from pathlib import Path


class PromptManager:
    """Manages predefined and custom prompts"""
    
    def __init__(self, prompts_file: Optional[str] = None):
        """
        Initialize Prompt Manager
        
        Args:
            prompts_file: Optional path to JSON file containing predefined prompts
        """
        self.logger = logging.getLogger(__name__)
        # Default to project‑root prompts.json (one level above this module)
        if prompts_file is not None:
            self.prompts_file = prompts_file
        else:
            project_root = Path(__file__).resolve().parents[1]
            self.prompts_file = str(project_root / "prompts.json")
        self.predefined_prompts: Dict[str, str] = {}
        self.load_predefined_prompts()
    
    def load_predefined_prompts(self):
        """Load predefined prompts from file or create default"""
        if os.path.exists(self.prompts_file):
            try:
                with open(self.prompts_file, 'r', encoding='utf-8') as f:
                    self.predefined_prompts = json.load(f)
                self.logger.info(f"Loaded {len(self.predefined_prompts)} predefined prompts")
                
                # Ensure "Document Processing" prompt exists (add if missing)
                default_prompts = self._get_default_prompts_dict()
                if "Document Processing" not in self.predefined_prompts:
                    self.predefined_prompts["Document Processing"] = default_prompts["Document Processing"]
                    # Move "Document Processing" to the beginning
                    new_prompts = {"Document Processing": self.predefined_prompts.pop("Document Processing")}
                    new_prompts.update(self.predefined_prompts)
                    self.predefined_prompts = new_prompts
                    self.save_prompts()
                    self.logger.info("Added 'Document Processing' prompt to existing prompts file")
            except Exception as e:
                self.logger.error(f"Error loading prompts file: {str(e)}")
                self._create_default_prompts()
        else:
            self._create_default_prompts()
            self.save_prompts()
    
    def _get_default_prompts_dict(self) -> Dict[str, str]:
        """Get default prompts as a dictionary (used for merging)"""
        return {
            # Main stage: Document Processing (Stage 1-4)
            "Document Processing": """You are a meticulous medical OCR & structuring assistant.



Your job is to extract ONLY what is present on each provided textbook page image/PDF

and convert it into clean, AI-ready text with structured sections.



You MUST avoid hallucinations and preserve original medical meaning precisely.



====================
GLOBAL PRINCIPLES
====================

- NO HALLUCINATIONS: Output only content visible on the page(s). If unreadable/unclear, write "[UNREADABLE]" or add an "uncertain" flag with brief reason.
- LANGUAGE & SPELLING: Preserve original English spelling and medical terminology.
- CLEAN OCR:

  - Remove headers/footers and running titles.

  - Fix broken words at line breaks.

  - Replace ligatures (ﬁ, ﬂ).

  - Preserve italics (*italic*), bold (**bold**), and subscripts/superscripts via LaTeX.

- CITATIONS: Keep in-text citations verbatim.

- MATH/CHEM: Use inline LaTeX.

- NO IMAGES: Describe figures textually only; do not invent labels.



====================
CRITICAL OUTPUT CONTRACT (MULTI-PART)
====================

The full output is TOO LARGE to fit in one response.



You MUST split the final output into multiple CHUNKS.



IMPORTANT:

- A CHUNK is ONLY a technical split of the RESPONSE due to size limits.

- CHUNKS are identified ONLY by the field "chunk_index".

- CHUNKS have NOTHING to do with the medical or logical structure of the chapter.



Each response MUST:

- Output ONLY valid JSON

- Contain ONLY ONE CHUNK of the final output

- NOT repeat rows from previous chunks

- Preserve the exact order of rows across chunks



====================
OUTPUT FORMAT (EXACT)
====================

{

  "chunk_index": <integer starting from 1>,

  "is_last": <true | false>,

  "rows": [

    {

      "Type": "page text | Figure | Table",

      "Extraction": "<single-line text or JSON>",

      "Number": <page number>,

      "Part": <integer>

    }

  ]

}



====================
MEANING OF chunk_index
====================

- "chunk_index" indicates ONLY the sequential number of this RESPONSE CHUNK.

- chunk_index = 1 means: first technical chunk of the output.

- chunk_index = 2 means: second technical chunk of the output.

- chunk_index has NO semantic meaning about the chapter content.

- chunk_index MUST strictly increase by 1 across responses.



====================
MEANING OF Part (VERY IMPORTANT)
====================

- "Part" represents the LOGICAL CONTENT PART of the chapter.

- "Part" is NOT related to chunk_index.

- "Part" is the result of grouping AUTHOR PARTS into YOUR OWN PARTS ("Our parts").



How to define "Part":

1) At the beginning of the chapter, there is a "Chapter contents" section that lists

   the author-defined parts ("author parts") and their page ranges.

2) You MUST group consecutive author parts into larger groups called "Our parts".

3) Each "Our part" MUST:

   - consist of consecutive author parts

   - NOT exceed 10 pages total

4) Assign integer labels starting from 1:

   - Our part 1

   - Our part 2

   - Our part 3

   - etc.



Example:

If author parts have page counts:

2, 4, 5, 3, 1, 7, 5



Then:

- Our part 1 = author parts 1 + 2

- Our part 2 = author parts 3 + 4 + 5

- Our part 3 = author part 6

- Our part 4 = author part 7



Rules for assigning "Part" in rows:

- Every row MUST have exactly ONE integer "Part" value.

- A single page MAY belong to TWO different "Part" values if an Our part boundary

  occurs in the middle of the page.

- In such cases, split the page text into TWO separate rows:

  - one row with the previous Part

  - one row with the next Part

- NEVER mix content from two different Parts inside the same row.
====================
PAGE-BY-PAGE FORWARD-ONLY PROCESSING (CRITICAL)
====================

You MUST process the document STRICTLY page by page, in ascending page order.

- NEVER go back to a previous page.

- NEVER re-extract or regenerate content from any page that has already appeared

  in earlier chunks.

- Once a page (or a portion of a page) is output in any chunk, it is considered FINAL.

- Subsequent chunks MUST continue ONLY from the next unprocessed page or page segment.

====================
Rules for rows
====================

- Each row represents exactly ONE of:

  - page text

  - figure

  - table

- Extraction MUST be single-line (no ENTER).

- JSON inside Extraction MUST be minified and valid.

- References section at end of chapter MUST be ignored.



====================
CHUNK INSTRUCTIONS
====================

This is CHUNK {chunk_index} of the output.



Generate ONLY the rows that belong to this CHUNK.

Stop when you reach a safe size limit for a single response.

If more content remains after this CHUNK, set:

  "is_last": false

Otherwise set:

  "is_last": true



====================
AUTHOR PARTS → OUR PARTS RULES
====================

[keep your full original rules here verbatim — unchanged]



====================
FINAL RULES
====================

- DO NOT summarize.

- DO NOT explain.

- DO NOT add commentary.

- Output ONLY the JSON object described above.



You will now receive a PDF.

Generate CHUNK {chunk_index}.""",
            
            # Generic prompts (still available)
            "Summarize Content": "Please provide a comprehensive summary of the following content. Include key points, main ideas, and important details.",
            
            "Extract Key Information": "Extract and organize the key information from the following content. Create a structured list of important points, facts, and data.",
            
            "Translate to English": "Translate the following content to English while maintaining the original meaning and context.",
            
            "Improve Writing": "Improve the following text for clarity, grammar, and professional tone. Maintain the original meaning while enhancing readability.",
            
            "Create Questions": "Based on the following content, create a set of comprehensive questions that test understanding of the material. Include both factual and analytical questions.",
            
            "Test Prompt": "This is a test prompt for content automation. Please analyze the provided content and provide a detailed response."
        }
    
    def _create_default_prompts(self):
        """Create default predefined prompts"""
        self.predefined_prompts = self._get_default_prompts_dict()
        self.logger.info("Created default predefined prompts")
    
    def save_prompts(self):
        """Save predefined prompts to file"""
        try:
            with open(self.prompts_file, 'w', encoding='utf-8') as f:
                json.dump(self.predefined_prompts, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Saved {len(self.predefined_prompts)} prompts to {self.prompts_file}")
        except Exception as e:
            self.logger.error(f"Error saving prompts: {str(e)}")
    
    def get_prompt_names(self) -> List[str]:
        """
        Get list of predefined prompt names
        
        Returns:
            List of prompt names
        """
        return list(self.predefined_prompts.keys())
    
    def get_prompt(self, name: str) -> Optional[str]:
        """
        Get predefined prompt by name
        
        Args:
            name: Name of the prompt
            
        Returns:
            Prompt text or None if not found
        """
        return self.predefined_prompts.get(name)
    
    def add_prompt(self, name: str, prompt: str):
        """
        Add a new predefined prompt
        
        Args:
            name: Name of the prompt
            prompt: Prompt text
        """
        self.predefined_prompts[name] = prompt
        self.save_prompts()
        self.logger.info(f"Added new prompt: {name}")
    
    def update_prompt(self, name: str, prompt: str):
        """
        Update an existing predefined prompt
        
        Args:
            name: Name of the prompt
            prompt: New prompt text
        """
        if name in self.predefined_prompts:
            self.predefined_prompts[name] = prompt
            self.save_prompts()
            self.logger.info(f"Updated prompt: {name}")
        else:
            self.logger.warning(f"Prompt '{name}' not found for update")
    
    def delete_prompt(self, name: str):
        """
        Delete a predefined prompt
        
        Args:
            name: Name of the prompt to delete
        """
        if name in self.predefined_prompts:
            del self.predefined_prompts[name]
            self.save_prompts()
            self.logger.info(f"Deleted prompt: {name}")
        else:
            self.logger.warning(f"Prompt '{name}' not found for deletion")











