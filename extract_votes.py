import os
import re
import pdfplumber
import pandas as pd
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
from unapproved_minutes import detect_unapproved_minutes, extend_item_text_for_unapproved

@dataclass
class VoteRecord:
    item_number: str
    motion_number: str
    motion_title: str
    motion_type: str  # 'Amendment' or 'Main Motion'
    legistar_number: str
    legistar_link: str
    description: str
    is_unanimous: bool
    ayes: List[str]
    ayes_count: int
    noes: List[str]
    noes_count: int
    abstentions: List[str]
    abstentions_count: int
    excused: List[str]
    excused_count: int
    recused: List[str]
    recused_count: int
    non_voting: List[str]
    non_voting_count: int
    page_number: int

class CommonCouncilVoteExtractor:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.vote_patterns = {
            'item_number': r'(?m)^\s*(\d+)\.\s+(\d+)',  # Matches "8. 78911" at start of line
            'sponsors': r'Sponsors?:\s*([^\n]+)',  # Matches "Sponsors: Name1, Name2"
            'motion_type': r'(?:Adopt the Following Amendment|Adopt(?:\s+Unanimously)?|(?:to\s+)?Call the\s+Question)',  # Different types of motions (handles newlines)
            'ayes': r'Ayes:\s*(\d+)\s*-\s*(.*?)(?=(?:\s+Noes:|Abstentions:|Recused:|Excused:|Non Voting:|$))',  # Matches "Ayes: 7- Names..."
            'noes': r'Noes:\s*(\d+)\s*-\s*(.*?)(?=(?:\s+Abstentions:|Recused:|Excused:|Non Voting:|$))',  # Matches "Noes: 12- Names..."
            'abstentions': r'Abstentions:\s*(\d+)\s*-\s*(.*?)(?=(?:\s+Recused:|Excused:|Non Voting:|$))',  # Matches "Abstentions: 1- Names..."
            'recused': r'Recused:\s*(\d+)\s*-\s*(.*?)(?=(?:\s+Excused:|Non Voting:|$))',  # Matches "Recused: 2- Names..."
            'excused': r'Excused:\s*(\d+)\s*-\s*(.*?)(?=(?:\s+Non Voting:|$))',  # Matches "Excused: 1- Names..."
            'non_voting': r'Non Voting:\s*(\d+)\s*-\s*([^;\n]+?)(?=(?:Enactment No:|City of Madison Page|\d{5,6}|$))',  # Simplified non-voting pattern
            'enactment': r'Enactment No:\s*([^\n]+)'  # Matches "Enactment No: ORD-24-00041"
        }
        # Don't open PDF here - open it in extract_votes with context manager

    def __del__(self):
        """Clean up PDF resources when object is destroyed."""
        if hasattr(self, 'pdf'):
            self.pdf.close()

    def extract_text_with_pages(self) -> List[Tuple[str, int]]:
        """Extract text from PDF with page numbers."""
        text_pages = []
        try:
            print(f"\nOpening PDF: {self.pdf_path}")
            with pdfplumber.open(self.pdf_path) as pdf:
                print(f"Successfully opened PDF. Total pages: {len(pdf.pages)}")
                for i, page in enumerate(pdf.pages, 1):
                    print(f"\nProcessing page {i}/{len(pdf.pages)}")
                    text = page.extract_text()
                    if text:
                        # Only include pages that might have votes
                        if any(pattern in text for pattern in ['Ayes:', 'Noes:', 'Adopt']):
                            print(f"Found potential vote information on page {i}")
                            text_pages.append((text, i))
                        else:
                            print(f"No vote information found on page {i}")
                    else:
                        print(f"Warning: No text extracted from page {i}")
        except Exception as e:
            print(f"Error extracting text from PDF: {e}")
        return text_pages

    def parse_names(self, names_str: str) -> List[str]:
        """Parse a string of names into a list."""
        if not names_str:
            return []
        
        # Clean up the input string
        names_str = names_str.strip()
        
        # Remove any text after specific markers that indicate end of names list
        markers = [
            r'Enactment No:',
            r'City of Madison Page',
            r'\d{5,6}',  # Legistar numbers
            r'REFER ALL',
            r'ADJOURN',
            r'SWEARING IN',
            r'CONVENE',
            r'ROLL CALL'
        ]
        for marker in markers:
            names_str = re.split(marker, names_str, flags=re.IGNORECASE)[0]
        
        # Handle common OCR/formatting issues
        names_str = re.sub(r'\s*and\s*', ';', names_str, flags=re.IGNORECASE)  # Replace " and " with semicolon
        names_str = re.sub(r'\s*;\s*', ';', names_str)  # Normalize semicolons
        
        # Split on semicolons
        names = []
        for part in names_str.split(';'):
            name = part.strip()
            if name:
                # Fix common OCR issues with names
                name = re.sub(r'\s+', ' ', name)  # Replace multiple spaces with single space
                name = re.sub(r'(?:Ayes|Noes|Excused|Recused|Non Voting):\s*\d+\s*-\s*', '', name)  # Remove vote type headers
                name = re.sub(r'[.,;]$', '', name)  # Remove trailing punctuation
                name = name.strip()
                if name and not any(keyword in name.lower() for keyword in [
                    'city of madison', 'page', 'substitute', 'sponsor',
                    'refer', 'adjourn', 'swearing', 'convene', 'roll call'
                ]):
                    names.append(name)
        
        return [n for n in names if n]  # Filter out any empty strings

    def find_votes_in_text(self, text: str) -> List[Dict]:
        """Find all vote sections in a text block."""
        votes = []
        current_pos = 0
        
        # Look for motion types that indicate a vote
        while True:
            # Find next motion type
            motion_match = re.search(self.vote_patterns['motion_type'], text[current_pos:])
            if not motion_match:
                break
                
            start_pos = current_pos + motion_match.start()
            # Find the next motion or end of text
            next_motion = re.search(self.vote_patterns['motion_type'], text[start_pos + 1:])
            end_pos = current_pos + len(text) if not next_motion else start_pos + next_motion.start()
            
            vote_text = text[start_pos:end_pos]
            motion_type = 'Amendment' if 'Amendment' in motion_match.group(0) else 'Main Motion'
            is_unanimous = 'Unanimously' in motion_match.group(0)
            
            vote_info = {
                'motion_title': motion_match.group(0),
                'motion_type': motion_type,
                'is_unanimous': is_unanimous,
                'text': vote_text
            }
            votes.append(vote_info)
            current_pos = start_pos + 1
            
        return votes

    def extract_votes(self, unapproved_minutes: bool = None) -> List[VoteRecord]:
        """
        Extract all votes from the PDF.
        
        Args:
            unapproved_minutes: If True, use special handling for unapproved minutes.
                               If False, use standard handling.
                               If None (default), automatically detect based on content.
        """
        if unapproved_minutes is None:
            unapproved_minutes = self._detect_unapproved_minutes()
        
        if unapproved_minutes:
            return self._extract_votes_unapproved()
        else:
            return self._extract_votes_approved()
    
    def _detect_unapproved_minutes(self) -> bool:
        """Detect if minutes are unapproved."""
        return detect_unapproved_minutes(self.pdf_path, self.vote_patterns)
    
    def _extract_votes_approved(self) -> List[VoteRecord]:
        """Extract votes from approved minutes."""
        return self._extract_votes_common(extend_item_boundaries=False)
    
    def _extract_votes_unapproved(self) -> List[VoteRecord]:
        """Extract votes from unapproved minutes with narrative format."""
        vote_records = []
        
        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                total_pages = len(pdf.pages)
                pages_text = []
                
                for page_num in range(total_pages):
                    try:
                        page = pdf.pages[page_num]
                        text = page.extract_text(layout=False)
                        if text is None:
                            text = ""
                        pages_text.append(text)
                    except Exception:
                        pages_text.append("")
                        continue
                
                full_text = "\n".join(pages_text)
                
                # Find all narrative motions: "A motion was made by X, seconded by Y, to [motion]"
                # Pattern handles newlines and various result formats
                # Using [\s\S] to match any character including newlines
                narrative_motion_pattern = r'A motion was made by\s+([^,\n]+),\s+seconded by\s+([^,\n]+),\s+to\s+([\s\S]+?)\.\s*The motion\s+([\s\S]+?)(?=\.|A motion was made by|\d+\.\s+\d{5,6}|City of Madison Page)'
                
                for page_num, text in enumerate(pages_text):
                    if not text:
                        continue
                    
                    # Find all items on this page
                    item_matches = list(re.finditer(self.vote_patterns['item_number'], text))
                    
                    if not item_matches:
                        continue
                    
                    # Find all narrative motions on this page
                    narrative_motions = list(re.finditer(narrative_motion_pattern, text, re.IGNORECASE | re.DOTALL))
                    
                    for narrative_match in narrative_motions:
                        motion_maker = narrative_match.group(1).strip()
                        motion_seconder = narrative_match.group(2).strip()
                        motion_text = narrative_match.group(3).strip()
                        motion_result = narrative_match.group(4).strip()
                        
                        # Find which item this motion belongs to
                        motion_pos = narrative_match.start()
                        item_num = None
                        legistar_num = None
                        
                        for item_match in item_matches:
                            if item_match.start() < motion_pos:
                                item_num = item_match.group(1)
                                legistar_num = item_match.group(2)
                            else:
                                break
                        
                        if not item_num or not legistar_num:
                            continue
                        
                        # Determine if unanimous
                        is_unanimous = (
                            'unanimously' in motion_result.lower() or 
                            'voice vote' in motion_result.lower() or
                            'voice vote/other' in motion_result.lower()
                        )
                        
                        # Extract motion type from motion text
                        motion_type = "Main Motion"
                        if 'adopt' in motion_text.lower():
                            if 'amendment' in motion_text.lower():
                                motion_type = "Amendment"
                            else:
                                motion_type = "Adopt"
                        elif 'call the question' in motion_text.lower() or 'call question' in motion_text.lower():
                            motion_type = "Call the Question"
                        elif 'refer' in motion_text.lower():
                            motion_type = "Refer"
                        elif 'adjourn' in motion_text.lower():
                            motion_type = "Adjourn"
                        
                        # Get the full text context for this motion
                        # Find the start of the item
                        item_start = None
                        for item_match in item_matches:
                            if item_match.group(1) == item_num:
                                item_start = item_match.start()
                                break
                        
                        if item_start is None:
                            continue
                        
                        # Find the end of this motion's context (next motion or next item)
                        next_narrative = re.search(narrative_motion_pattern, text[motion_pos + 1:], re.IGNORECASE | re.DOTALL)
                        next_item = None
                        for item_match in item_matches:
                            if item_match.start() > motion_pos:
                                next_item = item_match.start()
                                break
                        
                        if next_narrative:
                            motion_end = motion_pos + 1 + next_narrative.start()
                        elif next_item:
                            motion_end = next_item
                        else:
                            motion_end = min(len(text), item_start + 2000)
                        
                        # Extract full motion context including any vote details
                        motion_context = text[motion_pos:motion_end]
                        
                        # Check if there are explicit vote counts in the result
                        has_explicit_votes = any(vote_type in motion_result for vote_type in ['Ayes:', 'Noes:', 'Abstentions:', 'Recused:', 'Excused:', 'Non Voting:'])
                        
                        # If there are explicit votes, they might appear after "passed by the following vote:"
                        if has_explicit_votes or 'following vote' in motion_result.lower():
                            # Look for vote section after the narrative
                            vote_section_start = motion_pos + len(narrative_match.group(0))
                            vote_section = text[vote_section_start:motion_end]
                            
                            # Find where votes actually start (look for "Ayes:", "Noes:", etc.)
                            vote_start_match = re.search(r'(Ayes|Noes|Abstentions|Recused|Excused|Non Voting):\s*\d+\s*-', vote_section, re.IGNORECASE)
                            if vote_start_match:
                                # Include text from vote start
                                vote_section = vote_section[vote_start_match.start():]
                            
                            # Find where votes end (next motion, item, or page marker)
                            vote_end_match = re.search(r'(?=A motion was made by|\d+\.\s+\d{5,6}|City of Madison Page|Enactment No:)', vote_section, re.IGNORECASE)
                            if vote_end_match:
                                vote_section = vote_section[:vote_end_match.start()]
                            
                            # Combine narrative and vote section
                            full_motion_text = narrative_match.group(0) + "\n" + vote_section
                        else:
                            # For unanimous votes, just use the narrative text
                            full_motion_text = narrative_match.group(0)
                        
                        # Create motion title
                        motion_title = f"to {motion_text}"
                        
                        # Count motions for this item so far
                        motion_count = len([r for r in vote_records if r.item_number == item_num]) + 1
                        
                        # Process this motion
                        vote_record = self._process_item(
                            item_number=item_num,
                            legistar_number=legistar_num,
                            text=full_motion_text,
                            page_number=page_num + 1,
                            motion_number=str(motion_count),
                            motion_title=motion_title,
                            motion_type=motion_type,
                            is_unanimous=is_unanimous
                        )
                        
                        if vote_record:
                            vote_records.append(vote_record)
                
        except Exception as e:
            print(f"ERROR processing PDF: {e}")
            import traceback
            traceback.print_exc()
        
        return vote_records
    
    def _extract_votes_common(self, extend_item_boundaries: bool = False) -> List[VoteRecord]:
        """
        Common vote extraction logic.
        
        Args:
            extend_item_boundaries: If True, extend item_text to include votes that appear
                                   after the next item boundary
        """
        vote_records = []
        pending_vote = None
        
        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                total_pages = len(pdf.pages)
                pages_text = []
                
                for page_num in range(total_pages):
                    try:
                        page = pdf.pages[page_num]
                        text = page.extract_text(layout=False)
                        if text is None:
                            text = ""
                        pages_text.append(text)
                    except Exception as e:
                        pages_text.append("")
                        continue
                
                for page_num, text in enumerate(pages_text):
                    if not text or not any(pattern in text for pattern in ["Adopt", "Ayes:", "Noes:"]):
                        continue
                    
                    item_matches = list(re.finditer(self.vote_patterns['item_number'], text))
            
                    if item_matches:
                        for i, match in enumerate(item_matches):
                            item_num = match.group(1)
                            legistar_num = match.group(2)
                            
                            next_start = item_matches[i + 1].start() if i + 1 < len(item_matches) else len(text)
                            item_text = text[match.start():next_start]
                            
                            if extend_item_boundaries and i + 1 < len(item_matches):
                                item_text = extend_item_text_for_unapproved(
                                    item_text, item_num, text, match.start(), 
                                    item_matches[i + 1].start()
                                )
                    
                            if pending_vote and pending_vote['item_number'] == item_num:
                                pending_vote['text'] += "\n" + item_text
                                
                                if (('Ayes:' in pending_vote['text'] and 
                                     any(marker in item_text for marker in ['City of Madison Page', 'Adopt'])) or
                                    all(marker in pending_vote['text'] for marker in ['Ayes:', 'Noes:', 'Excused:', 'Non Voting:'])):
                                    vote_record = self._process_item(
                                        item_number=pending_vote['item_number'],
                                        legistar_number=pending_vote['legistar_number'],
                                        text=pending_vote['text'],
                                        page_number=page_num + 1,
                                        motion_number=pending_vote['motion_number'],
                                        motion_title=pending_vote['motion_title'],
                                        motion_type=pending_vote['motion_type'],
                                        is_unanimous=pending_vote['is_unanimous']
                                    )
                                    if vote_record:
                                        vote_records.append(vote_record)
                                    pending_vote = None
                                continue
                            
                            motion_number = 0
                            remaining_text = item_text
                            max_motions = 100
                            prev_remaining_length = len(remaining_text)
                            
                            while motion_number < max_motions:
                                motion_match = re.search(self.vote_patterns['motion_type'], remaining_text)
                                if not motion_match:
                                    break
                                    
                                motion_number += 1
                                motion_start = motion_match.start()
                                motion_type = motion_match.group(0)
                                
                                next_motion = re.search(self.vote_patterns['motion_type'], remaining_text[motion_start + 1:])
                                if next_motion:
                                    next_motion_abs_pos = motion_start + 1 + next_motion.start()
                                    
                                    current_motion_normalized = motion_type.replace('to ', '').replace('\n', ' ').strip().lower()
                                    next_motion_normalized = next_motion.group(0).replace('to ', '').replace('\n', ' ').strip().lower()
                                    
                                    if current_motion_normalized == next_motion_normalized or next_motion_normalized in current_motion_normalized or current_motion_normalized in next_motion_normalized:
                                        remaining_text = remaining_text[next_motion_abs_pos:]
                                        continue
                                    else:
                                        text_up_to_next = remaining_text[motion_start:next_motion_abs_pos]
                                    
                                    has_vote_in_text = any(vote_type in text_up_to_next for vote_type in ['Ayes:', 'Noes:', 'Abstentions:', 'Recused:', 'Excused:', 'Non Voting:'])
                                    
                                    if has_vote_in_text:
                                        text_after_motion = remaining_text[motion_start:]
                                        vote_end_marker = re.search(r'(?=A motion was made by|End of|Business Presented|City of Madison Page|\n\d+\.\s+\d{5,6}|' + re.escape(next_motion.group(0)) + r')', text_after_motion, re.IGNORECASE)
                                        if vote_end_marker and vote_end_marker.start() < next_motion_abs_pos - motion_start:
                                            motion_text = remaining_text[motion_start:motion_start + vote_end_marker.start()]
                                            new_remaining = remaining_text[motion_start + vote_end_marker.start():]
                                        else:
                                            motion_text = text_up_to_next
                                            new_remaining = remaining_text[next_motion_abs_pos:]
                                    else:
                                        motion_text = text_up_to_next
                                        new_remaining = remaining_text[next_motion_abs_pos:]
                                    
                                    if len(new_remaining) >= len(remaining_text):
                                        break
                                    remaining_text = new_remaining
                                else:
                                    motion_text = remaining_text[motion_start:]
                                    remaining_text = ""
                                
                                is_unanimous = "Unanimously" in motion_type
                                
                                has_ayes = 'Ayes:' in motion_text
                                has_noes = 'Noes:' in motion_text
                                has_non_voting = 'Non Voting:' in motion_text
                                
                                is_complete = (
                                    not has_ayes or
                                    (has_ayes and (has_noes or has_non_voting)) or
                                    is_unanimous or
                                    'voice vote' in motion_text.lower()
                                )
                                
                                if has_ayes and not is_complete:
                                    pending_vote = {
                                        'item_number': item_num,
                                        'legistar_number': legistar_num,
                                        'text': motion_text,
                                        'motion_number': str(motion_number),
                                        'motion_title': motion_type,
                                        'motion_type': "Main Motion",
                                        'is_unanimous': is_unanimous
                                    }
                                elif is_complete:
                                    vote_record = self._process_item(
                                        item_number=item_num,
                                        legistar_number=legistar_num,
                                        text=motion_text,
                                        page_number=page_num + 1,
                                        motion_number=str(motion_number),
                                        motion_title=motion_type,
                                        motion_type="Main Motion",
                                        is_unanimous=is_unanimous
                                    )
                                    
                                    if vote_record:
                                        vote_records.append(vote_record)
                                
                                if not remaining_text:
                                    break
                                
                                if len(remaining_text) == prev_remaining_length:
                                    break
                                prev_remaining_length = len(remaining_text)
                
        except Exception as e:
            print(f"ERROR processing PDF: {e}")
            import traceback
            traceback.print_exc()
        
        return vote_records
    

    def _process_item(self, item_number: str, legistar_number: str, text: str, 
                     page_number: int, motion_number: str, motion_title: str,
                     motion_type: str, is_unanimous: bool) -> Optional[VoteRecord]:
        """Process a single agenda item text to extract vote information."""
        legistar_link = f"https://madison.legistar.com/gateway.aspx?m=l&id=/matter.aspx?key={legistar_number}"
        description = text.split(motion_title)[0].strip() if motion_title in text else ""
        
        # Initialize vote counts and lists
        ayes = []
        ayes_count = 0
        noes = []
        noes_count = 0
        abstentions = []
        abstentions_count = 0
        excused = []
        excused_count = 0
        recused = []
        recused_count = 0
        non_voting = []
        non_voting_count = 0
        
        if is_unanimous:
            ayes = ["UNANIMOUS"]
            ayes_count = -1
        else:
            vote_section_end = None
            for pattern in [
                r'(?:Ayes|Noes|Abstentions|Recused|Excused|Non Voting):\s*\d+\s*-.*?(?=Enactment No:)',
                r'(?:Ayes|Noes|Abstentions|Recused|Excused|Non Voting):\s*\d+\s*-.*?(?=City of Madison Page)'
            ]:
                try:
                    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
                    if match:
                        vote_section_end = match.end()
                        break
                except Exception:
                    pass
            
            if vote_section_end:
                boundaries = ['ROLL CALL', 'SWEARING IN', 'CONVENE', 'ADJOURN', 'REFER ALL']
                remaining_text = text[vote_section_end:]
                for boundary in boundaries:
                    try:
                        match = re.search(boundary, remaining_text, re.IGNORECASE)
                        if match:
                            text = text[:vote_section_end + match.start()]
                            break
                    except Exception:
                        pass
            
            max_text_size = 50000
            text_to_split = text[:max_text_size] if len(text) > max_text_size else text
            try:
                sections = re.split(r'((?:Ayes|Noes|Abstentions|Recused|Excused|Non Voting):\s*\d+\s*-\s*)', text_to_split)
            except Exception:
                sections = []
            
            current_section = None
            current_names = []
            max_sections = 200
            sections_to_process = sections[:max_sections] if len(sections) > max_sections else sections
            
            for i, section in enumerate(sections_to_process):
                if not section.strip():
                    continue
                
                try:
                    header_match = re.match(r'(Ayes|Noes|Abstentions|Recused|Excused|Non Voting):\s*(\d+)\s*-\s*', section)
                    if header_match:
                        if current_section and current_names:
                            names = self.parse_names(' '.join(current_names))
                            if current_section == 'Ayes':
                                ayes.extend(names)
                            elif current_section == 'Noes':
                                noes.extend(names)
                            elif current_section == 'Abstentions':
                                abstentions.extend(names)
                            elif current_section == 'Recused':
                                recused.extend(names)
                            elif current_section == 'Excused':
                                excused.extend(names)
                            elif current_section == 'Non Voting':
                                non_voting.extend(names)
                        
                        current_section = header_match.group(1)
                        count = int(header_match.group(2))
                        if current_section == 'Ayes':
                            ayes_count = count
                        elif current_section == 'Noes':
                            noes_count = count
                        elif current_section == 'Abstentions':
                            abstentions_count = count
                        elif current_section == 'Recused':
                            recused_count = count
                        elif current_section == 'Excused':
                            excused_count = count
                        elif current_section == 'Non Voting':
                            non_voting_count = count
                        current_names = []
                    else:
                        if current_section:
                            current_names.append(section)
                except Exception:
                    pass
            
            if current_section and current_names:
                try:
                    names = self.parse_names(' '.join(current_names))
                    if current_section == 'Ayes':
                        ayes.extend(names)
                    elif current_section == 'Noes':
                        noes.extend(names)
                    elif current_section == 'Abstentions':
                        abstentions.extend(names)
                    elif current_section == 'Recused':
                        recused.extend(names)
                    elif current_section == 'Excused':
                        excused.extend(names)
                    elif current_section == 'Non Voting':
                        non_voting.extend(names)
                except Exception:
                    pass
            
            if non_voting_count > 0 and not non_voting:
                try:
                    non_voting_match = re.search(r'Non Voting:\s*\d+\s*-\s*([^;\n]+?)(?=(?:Enactment No:|City of Madison Page|\d{5,6}|$))', text)
                    if non_voting_match:
                        non_voting = self.parse_names(non_voting_match.group(1))
                except Exception:
                    pass
        
        # Return a record if we have any vote information
        if is_unanimous or any([ayes, noes, abstentions, recused, excused, non_voting]):
            return VoteRecord(
                item_number=item_number,
                motion_number=motion_number,
                motion_title=motion_title.strip(),
                motion_type=motion_type,
                legistar_number=legistar_number,
                legistar_link=legistar_link,
                description=description,
                is_unanimous=is_unanimous,
                ayes=ayes,
                ayes_count=ayes_count,
                noes=noes,
                noes_count=noes_count,
                abstentions=abstentions,
                abstentions_count=abstentions_count,
                excused=excused,
                excused_count=excused_count,
                recused=recused,
                recused_count=recused_count,
                non_voting=non_voting,
                non_voting_count=non_voting_count,
                page_number=page_number
            )
        return None

def process_single_pdf(pdf_path: str, unapproved_minutes: bool = None):
    """
    Process a single Common Council minutes PDF.
    
    Args:
        pdf_path: Path to the PDF file
        unapproved_minutes: If True, use special handling for unapproved minutes.
                          If False, use standard handling.
                          If None (default), automatically detect based on content.
    """
    extractor = CommonCouncilVoteExtractor(pdf_path)
    vote_records = extractor.extract_votes(unapproved_minutes=unapproved_minutes)
    
    if vote_records:
        # Generate two output files - summary and detailed
        summary_results = []
        detailed_results = []
        
        for record in vote_records:
            # Summary record (one per vote)
            summary_record = {
                'item_number': record.item_number,
                'motion_number': record.motion_number,
                'motion_title': record.motion_title.strip(),
                'motion_type': record.motion_type,
                'legistar_number': record.legistar_number,
                'legistar_link': record.legistar_link,
                'description': record.description.strip(),
                'is_unanimous': record.is_unanimous,
                'total_ayes': record.ayes_count if record.ayes_count != -1 else len(record.ayes),
                'total_noes': record.noes_count,
                'total_abstentions': record.abstentions_count,
                'total_excused': record.excused_count,
                'total_recused': record.recused_count,
                'total_non_voting': record.non_voting_count,
                'page_number': record.page_number
            }
            summary_results.append(summary_record)
            
            # Detailed records (one per person per vote)
            vote_date = Path(pdf_path).stem  # Get date from filename
            
            if record.is_unanimous:
                # For unanimous votes, we don't have individual names
                # but we know everyone present voted aye
                detailed_results.append({
                    'date': vote_date,
                    'item_number': record.item_number,
                    'motion_number': record.motion_number,
                    'motion_type': record.motion_type,
                    'legistar_number': record.legistar_number,
                    'member_name': 'ALL_PRESENT',  # Special marker for unanimous votes
                    'vote_type': 'UNANIMOUS_AYE',
                    'is_unanimous': True
                })
            else:
                # Process individual votes for non-unanimous votes
                for name in record.ayes:
                    if name != "UNANIMOUS":  # Skip the special unanimous marker
                        detailed_results.append({
                            'date': vote_date,
                            'item_number': record.item_number,
                            'motion_number': record.motion_number,
                            'motion_type': record.motion_type,
                            'legistar_number': record.legistar_number,
                            'member_name': name.strip(),
                            'vote_type': 'AYE',
                            'is_unanimous': False
                        })
                
                for name in record.noes:
                    detailed_results.append({
                        'date': vote_date,
                        'item_number': record.item_number,
                        'motion_number': record.motion_number,
                        'motion_type': record.motion_type,
                        'legistar_number': record.legistar_number,
                        'member_name': name.strip(),
                        'vote_type': 'NO',
                        'is_unanimous': False
                    })
                
                for name in record.abstentions:
                    detailed_results.append({
                        'date': vote_date,
                        'item_number': record.item_number,
                        'motion_number': record.motion_number,
                        'motion_type': record.motion_type,
                        'legistar_number': record.legistar_number,
                        'member_name': name.strip(),
                        'vote_type': 'ABSTAIN',
                        'is_unanimous': False
                    })
                
                for name in record.excused:
                    detailed_results.append({
                        'date': vote_date,
                        'item_number': record.item_number,
                        'motion_number': record.motion_number,
                        'motion_type': record.motion_type,
                        'legistar_number': record.legistar_number,
                        'member_name': name.strip(),
                        'vote_type': 'EXCUSED',
                        'is_unanimous': False
                    })
                
                for name in record.recused:
                    detailed_results.append({
                        'date': vote_date,
                        'item_number': record.item_number,
                        'motion_number': record.motion_number,
                        'motion_type': record.motion_type,
                        'legistar_number': record.legistar_number,
                        'member_name': name.strip(),
                        'vote_type': 'RECUSED',
                        'is_unanimous': False
                    })
                
                for name in record.non_voting:
                    detailed_results.append({
                        'date': vote_date,
                        'item_number': record.item_number,
                        'motion_number': record.motion_number,
                        'motion_type': record.motion_type,
                        'legistar_number': record.legistar_number,
                        'member_name': name.strip(),
                        'vote_type': 'NON_VOTING',
                        'is_unanimous': False
                    })
        
        # Save summary file with clean formatting
        summary_df = pd.DataFrame(summary_results)
        summary_output = pdf_path.rsplit('.', 1)[0] + '_votes_summary.csv'
        summary_df.to_csv(summary_output, index=False)
        
        # Save detailed file with clean formatting
        detailed_df = pd.DataFrame(detailed_results)
        detailed_output = pdf_path.rsplit('.', 1)[0] + '_votes_detailed.csv'
        detailed_df.to_csv(detailed_output, index=False)
        
        print(f"Extracted {len(summary_results)} vote records")
        print(f"Summary results saved to: {summary_output}")
        print(f"Detailed results saved to: {detailed_output}")
    else:
        print("No vote records found")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        pdf_path = sys.argv[1]
        process_single_pdf(pdf_path)
    else:
        print("Usage: python extract_votes.py <pdf_path>") 