"""
Module for handling unapproved (non-finalized) Common Council meeting minutes.

Unapproved minutes have a more narrative format with additional text between the item 
number and the votes. The key difference is the presence of narrative text like:
"A motion was made by X, seconded by Y, to [motion]. The motion passed..."

Approved minutes have a more direct format where motion types appear directly without
the narrative wrapper.

Example unapproved format:
  4. 90249 Item description
     A motion was made by Vidaver, seconded by Govindarajan, to Adopt Floor Amendment #1.
     The motion passed unanimously by voice vote/other.
     A motion was made by O'Brien, seconded by Govindarajan, to Call the Question.
     The motion passed by the following vote:
     Ayes: 17 - Names...
     
This module provides detection and text extension utilities for this format.
"""

import re
import pdfplumber
from typing import Dict


def detect_unapproved_minutes(pdf_path: str, vote_patterns: Dict[str, str]) -> bool:
    """
    Detect if minutes are unapproved by checking for narrative text patterns.
    
    Unapproved minutes have a more narrative format with text like:
    "A motion was made by X, seconded by Y, to [motion]. The motion passed..."
    
    Approved minutes have a more direct format with motion types appearing directly.
    
    Args:
        pdf_path: Path to the PDF file
        vote_patterns: Dictionary containing regex patterns (not used but kept for compatibility)
    
    Returns:
        True if unapproved minutes format detected, False otherwise
    """
    try:
        with pdfplumber.open(pdf_path) as pdf:
            sample_pages = min(5, len(pdf.pages))
            narrative_indicators = 0
            
            for page_num in range(sample_pages):
                try:
                    page = pdf.pages[page_num]
                    text = page.extract_text(layout=False)
                    if not text:
                        continue
                    
                    narrative_pattern = r'A motion was made by\s+\w+,\s+seconded by\s+\w+'
                    narrative_matches = list(re.finditer(narrative_pattern, text, re.IGNORECASE))
                    
                    if narrative_matches:
                        item_matches = list(re.finditer(vote_patterns['item_number'], text))
                        
                        for narrative_match in narrative_matches:
                            narrative_pos = narrative_match.start()
                            nearest_item = None
                            for item_match in item_matches:
                                if item_match.start() < narrative_pos:
                                    if nearest_item is None or item_match.start() > nearest_item.start():
                                        nearest_item = item_match
                            
                            if nearest_item:
                                narrative_indicators += 1
                                break
                        if narrative_indicators > 0:
                            break
                except Exception:
                    continue
            
            return narrative_indicators > 0
    except Exception:
        return False


def extend_item_text_for_unapproved(
    item_text: str, 
    item_num: str, 
    full_text: str, 
    item_start: int, 
    next_item_start: int
) -> str:
    """
    For unapproved minutes: extend item_text to include votes that appear 
    after the next item boundary.
    
    Args:
        item_text: Current item text
        item_num: Item number for logging
        full_text: Full page text
        item_start: Start position of current item in full_text
        next_item_start: Start position of next item in full_text
        
    Returns:
        Extended item_text if votes found, otherwise original item_text
    """
    text_after_next_item = full_text[next_item_start:min(len(full_text), next_item_start + 1000)]
    vote_after_next = re.search(r'(Ayes|Noes|Abstentions|Recused|Excused|Non Voting):\s*\d+\s*-', text_after_next_item)
    if vote_after_next and ('Ayes:' in item_text or 'Noes:' in item_text):
        vote_end = re.search(r'(?=A motion was made by|End of|Business Presented|City of Madison Page|\n\d+\.\s+\d{5,6})', text_after_next_item, re.IGNORECASE)
        if vote_end:
            extended_text = full_text[item_start:next_item_start + vote_end.start()]
            return extended_text
    
    return item_text

