import os
import re
import pdfplumber
import pandas as pd
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path

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
            'motion_type': r'(?:Adopt the Following Amendment|Adopt(?:\s+Unanimously)?)',  # Different types of motions
            'ayes': r'Ayes:\s*(\d+)\s*-\s*(.*?)(?=(?:\s+Noes:|Abstentions:|Recused:|Excused:|Non Voting:|$))',  # Matches "Ayes: 7- Names..."
            'noes': r'Noes:\s*(\d+)\s*-\s*(.*?)(?=(?:\s+Abstentions:|Recused:|Excused:|Non Voting:|$))',  # Matches "Noes: 12- Names..."
            'abstentions': r'Abstentions:\s*(\d+)\s*-\s*(.*?)(?=(?:\s+Recused:|Excused:|Non Voting:|$))',  # Matches "Abstentions: 1- Names..."
            'recused': r'Recused:\s*(\d+)\s*-\s*(.*?)(?=(?:\s+Excused:|Non Voting:|$))',  # Matches "Recused: 2- Names..."
            'excused': r'Excused:\s*(\d+)\s*-\s*(.*?)(?=(?:\s+Non Voting:|$))',  # Matches "Excused: 1- Names..."
            'non_voting': r'Non Voting:\s*(\d+)\s*-\s*([^;\n]+?)(?=(?:Enactment No:|City of Madison Page|\d{5,6}|$))',  # Simplified non-voting pattern
            'enactment': r'Enactment No:\s*([^\n]+)'  # Matches "Enactment No: ORD-24-00041"
        }
        self.pdf = pdfplumber.open(pdf_path)

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

    def extract_votes(self) -> List[VoteRecord]:
        """Extract all votes from the PDF."""
        vote_records = []
        pending_vote = None  # Track votes that might continue on next page
        
        # First pass: collect all text and identify page boundaries
        pages_text = []
        for page_num in range(len(self.pdf.pages)):
            page = self.pdf.pages[page_num]
            text = page.extract_text()
            pages_text.append(text)
            
            print(f"\nProcessing page {page_num + 1}/{len(self.pdf.pages)}")
            
            # Skip pages without vote information
            if not any(pattern in text for pattern in ["Adopt", "Ayes:", "Noes:"]):
                print("No vote information found on page", page_num + 1)
                continue
                
            print("Found potential vote information on page", page_num + 1)
            
            # Find all agenda items with their Legistar numbers
            item_matches = list(re.finditer(self.vote_patterns['item_number'], text))
            
            if item_matches:
                print(f"\nFound {len(item_matches)} agenda items on page {page_num + 1}")
                
                for i, match in enumerate(item_matches):
                    item_num = match.group(1)
                    legistar_num = match.group(2)
                    
                    # Get text until next item or end of text
                    next_start = item_matches[i + 1].start() if i + 1 < len(item_matches) else len(text)
                    item_text = text[match.start():next_start]
                    
                    # Check if this is a continuation of a pending vote
                    if pending_vote and pending_vote['item_number'] == item_num:
                        # This is a continuation - append the text
                        pending_vote['text'] += "\n" + item_text
                        
                        # If we find a complete vote section (has both Ayes and either end marker or new motion),
                        # process it and clear pending
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
                                print(f"Found complete vote record for item {item_num}, vote {pending_vote['motion_number']} ({vote_record.motion_type})")
                            pending_vote = None
                        continue
                    
                    # Find all motions in this item
                    motion_number = 0
                    remaining_text = item_text
                    while True:
                        # Find next motion type
                        motion_match = re.search(self.vote_patterns['motion_type'], remaining_text)
                        if not motion_match:
                            break
                            
                        motion_number += 1
                        motion_start = motion_match.start()
                        motion_type = motion_match.group(0)
                        
                        # Get text until next motion or end of item
                        next_motion = re.search(self.vote_patterns['motion_type'], remaining_text[motion_start + 1:])
                        if next_motion:
                            motion_text = remaining_text[motion_start:motion_start + next_motion.start() + 1]
                            remaining_text = remaining_text[motion_start + next_motion.start() + 1:]
                        else:
                            motion_text = remaining_text[motion_start:]
                            remaining_text = ""
                        
                        # Check if this is a unanimous vote
                        is_unanimous = "Unanimously" in motion_type
                        
                        # Check if this vote might continue on next page
                        if ('Ayes:' in motion_text and 
                            not all(marker in motion_text for marker in ['Noes:', 'Excused:', 'Non Voting:'])):
                            # This vote might continue - save it as pending
                            pending_vote = {
                                'item_number': item_num,
                                'legistar_number': legistar_num,
                                'text': motion_text,
                                'motion_number': str(motion_number),
                                'motion_title': motion_type,
                                'motion_type': "Main Motion",
                                'is_unanimous': is_unanimous
                            }
                            print(f"Found potentially incomplete vote for item {item_num}, vote {motion_number}")
                        else:
                            # Process the complete vote
                            vote_record = self._process_item(
                                item_number=item_num,
                                legistar_number=legistar_num,
                                text=motion_text,
                                page_number=page_num + 1,
                                motion_number=str(motion_number),
                                motion_title=motion_type,
                                motion_type="Main Motion",  # We'll improve this later
                                is_unanimous=is_unanimous
                            )
                            
                            if vote_record:
                                vote_records.append(vote_record)
                                print(f"Found complete vote record for item {item_num}, vote {motion_number} ({vote_record.motion_type})")
                        
                        if not remaining_text:
                            break
        
        print(f"\nTotal vote records found: {len(vote_records)}")
        return vote_records

    def _process_item(self, item_number: str, legistar_number: str, text: str, 
                     page_number: int, motion_number: str, motion_title: str,
                     motion_type: str, is_unanimous: bool) -> Optional[VoteRecord]:
        """Process a single agenda item text to extract vote information."""
        print(f"\nProcessing {motion_type} vote {motion_number} for item {item_number}")
        print(f"Vote text length: {len(text)} characters")
        
        # Create Legistar link
        legistar_link = f"https://madison.legistar.com/gateway.aspx?m=l&id=/matter.aspx?key={legistar_number}"
        
        # Extract description (text before the motion)
        description = text.split(motion_title)[0].strip() if motion_title in text else ""
        # Extract description (text before the motion)
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
        
        # For unanimous votes without explicit counts, we'll mark it specially
        if is_unanimous:
            print("Found unanimous vote")
            ayes = ["UNANIMOUS"]
            ayes_count = -1  # Special marker for unanimous
            
        else:
            print("Processing non-unanimous vote...")
            
            # Only trim the text if we find a complete vote section followed by unrelated content
            print("Looking for vote section boundaries...")
            vote_section_end = None
            for pattern in [
                r'(?:Ayes|Noes|Abstentions|Recused|Excused|Non Voting):\s*\d+\s*-.*?(?=Enactment No:)',
                r'(?:Ayes|Noes|Abstentions|Recused|Excused|Non Voting):\s*\d+\s*-.*?(?=City of Madison Page)'
            ]:
                try:
                    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
                    if match:
                        vote_section_end = match.end()
                        print(f"Found vote section ending at position {vote_section_end}")
                        break
                except Exception as e:
                    print(f"Error searching for vote section: {e}")
            
            if vote_section_end:
                print("Looking for boundaries after vote section...")
                # Look for boundaries after the vote section
                boundaries = [
                    'ROLL CALL',
                    'SWEARING IN',
                    'CONVENE',
                    'ADJOURN',
                    'REFER ALL'
                ]
                remaining_text = text[vote_section_end:]
                print(f"Remaining text length: {len(remaining_text)} characters")
                for boundary in boundaries:
                    try:
                        match = re.search(boundary, remaining_text, re.IGNORECASE)
                        if match:
                            text = text[:vote_section_end + match.start()]
                            print(f"Trimmed text at boundary '{boundary}'")
                            break
                    except Exception as e:
                        print(f"Error searching for boundary '{boundary}': {e}")
            
            print("Splitting text into vote sections...")
            # Split text into sections based on vote type headers
            try:
                sections = re.split(r'((?:Ayes|Noes|Abstentions|Recused|Excused|Non Voting):\s*\d+\s*-\s*)', text)
                print(f"Found {len(sections)} sections")
            except Exception as e:
                print(f"Error splitting text into sections: {e}")
                sections = []
            
            current_section = None
            current_names = []
            
            for i, section in enumerate(sections):
                print(f"Processing section {i+1}/{len(sections)} (length: {len(section)} chars)")
                if not section.strip():
                    continue
                
                # Check if this is a header
                try:
                    header_match = re.match(r'(Ayes|Noes|Abstentions|Recused|Excused|Non Voting):\s*(\d+)\s*-\s*', section)
                    if header_match:
                        print(f"Found header: {header_match.group(1)} with count {header_match.group(2)}")
                        # Process previous section if exists
                        if current_section and current_names:
                            names = self.parse_names(' '.join(current_names))
                            print(f"Processing {len(names)} names from {current_section}")
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
                        
                        # Start new section
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
                        # This is content, add to current section
                        if current_section:
                            current_names.append(section)
                except Exception as e:
                    print(f"Error processing section {i+1}: {e}")
            
            # Process the last section
            if current_section and current_names:
                try:
                    names = self.parse_names(' '.join(current_names))
                    print(f"Processing final section: {current_section} with {len(names)} names")
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
                except Exception as e:
                    print(f"Error processing final section: {e}")
            
            # If we have a non-voting count but no names, try to extract from the text
            if non_voting_count > 0 and not non_voting:
                print("Attempting to extract missing non-voting names...")
                try:
                    non_voting_match = re.search(r'Non Voting:\s*\d+\s*-\s*([^;\n]+?)(?=(?:Enactment No:|City of Madison Page|\d{5,6}|$))', text)
                    if non_voting_match:
                        non_voting = self.parse_names(non_voting_match.group(1))
                        print(f"Found {len(non_voting)} non-voting names in second pass")
                except Exception as e:
                    print(f"Error extracting non-voting names: {e}")
            
            if ayes:
                print(f"Found {len(ayes)} ayes (count: {ayes_count}): {ayes}")
            if noes:
                print(f"Found {len(noes)} noes (count: {noes_count}): {noes}")
            if abstentions:
                print(f"Found {len(abstentions)} abstentions (count: {abstentions_count}): {abstentions}")
            if recused:
                print(f"Found {len(recused)} recused (count: {recused_count}): {recused}")
            if excused:
                print(f"Found {len(excused)} excused (count: {excused_count}): {excused}")
            if non_voting:
                print(f"Found {len(non_voting)} non-voting (count: {non_voting_count}): {non_voting}")
        
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

def process_single_pdf(pdf_path: str):
    """Process a single Common Council minutes PDF."""
    print(f"\nProcessing single PDF: {pdf_path}")
    extractor = CommonCouncilVoteExtractor(pdf_path)
    vote_records = extractor.extract_votes()
    
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
        
        print(f"\nExtracted {len(summary_results)} vote records")
        print(f"Generated {len(detailed_results)} individual vote records")
        print(f"Summary results saved to: {summary_output}")
        print(f"Detailed results saved to: {detailed_output}")
        
        # Validate vote counts
        print("\nValidating vote counts...")
        
        # Print non-unanimous votes for verification
        print("\nNon-unanimous votes found:")
        vote_details = {}  # Store vote details for comparison
        
        for record in summary_results:
            if not record['is_unanimous']:
                vote_id = f"{record['item_number']}_{record['motion_number']}"
                vote_sum = (
                    record['total_ayes'] +
                    record['total_noes'] +
                    record['total_abstentions'] +
                    record['total_excused'] +
                    record['total_recused'] +
                    record['total_non_voting']
                )
                
                print(f"\nItem {record['item_number']}, Motion {record['motion_number']}:")
                print(f"  Title: {record['motion_title']}")
                print(f"  Ayes: {record['total_ayes']}")
                print(f"  Noes: {record['total_noes']}")
                print(f"  Abstentions: {record['total_abstentions']}")
                print(f"  Excused: {record['total_excused']}")
                print(f"  Recused: {record['total_recused']}")
                print(f"  Non-voting: {record['total_non_voting']}")
                print(f"  Total votes: {vote_sum}")
                
                vote_details[vote_id] = {
                    'summary_total': vote_sum,
                    'ayes': record['total_ayes'],
                    'noes': record['total_noes'],
                    'abstentions': record['total_abstentions'],
                    'excused': record['total_excused'],
                    'recused': record['total_recused'],
                    'non_voting': record['total_non_voting']
                }
        
        print(f"\nFound {len([r for r in summary_results if r['is_unanimous']])} unanimous votes and {len([r for r in summary_results if not r['is_unanimous']])} non-unanimous votes")
        
        # Count non-unanimous votes in detailed
        non_unanimous_votes = [vote for vote in detailed_results if not vote['is_unanimous']]
        
        # Group detailed votes by item and motion
        detailed_vote_counts = {}
        for vote in non_unanimous_votes:
            vote_id = f"{vote['item_number']}_{vote['motion_number']}"
            if vote_id not in detailed_vote_counts:
                detailed_vote_counts[vote_id] = {'AYE': 0, 'NO': 0, 'ABSTAIN': 0, 'EXCUSED': 0, 'RECUSED': 0, 'NON_VOTING': 0}
            detailed_vote_counts[vote_id][vote['vote_type']] += 1
        
        # Compare vote counts for each item
        print("\nDetailed vote count comparison:")
        total_non_unanimous_in_summary = 0
        total_non_unanimous_in_detailed = 0
        
        for vote_id in sorted(set(vote_details.keys()) | set(detailed_vote_counts.keys())):
            print(f"\nVote {vote_id}:")
            if vote_id in vote_details:
                summary = vote_details[vote_id]
                print("Summary counts:")
                print(f"  Ayes: {summary['ayes']}")
                print(f"  Noes: {summary['noes']}")
                print(f"  Abstentions: {summary['abstentions']}")
                print(f"  Excused: {summary['excused']}")
                print(f"  Recused: {summary['recused']}")
                print(f"  Non-voting: {summary['non_voting']}")
                print(f"  Total: {summary['summary_total']}")
                total_non_unanimous_in_summary += summary['summary_total']
            
            if vote_id in detailed_vote_counts:
                detailed = detailed_vote_counts[vote_id]
                print("Detailed counts:")
                print(f"  Ayes: {detailed['AYE']}")
                print(f"  Noes: {detailed['NO']}")
                print(f"  Abstentions: {detailed['ABSTAIN']}")
                print(f"  Excused: {detailed['EXCUSED']}")
                print(f"  Recused: {detailed['RECUSED']}")
                print(f"  Non-voting: {detailed['NON_VOTING']}")
                detailed_total = sum(detailed.values())
                print(f"  Total: {detailed_total}")
                total_non_unanimous_in_detailed += detailed_total
            
            if vote_id in vote_details and vote_id in detailed_vote_counts:
                summary_total = vote_details[vote_id]['summary_total']
                detailed_total = sum(detailed_vote_counts[vote_id].values())
                if summary_total != detailed_total:
                    print(f"⚠ Mismatch for vote {vote_id}: Summary={summary_total}, Detailed={detailed_total}")
                    print("  Differences:")
                    if vote_details[vote_id]['ayes'] != detailed_vote_counts[vote_id]['AYE']:
                        print(f"    Ayes: Summary={vote_details[vote_id]['ayes']}, Detailed={detailed_vote_counts[vote_id]['AYE']}")
                    if vote_details[vote_id]['noes'] != detailed_vote_counts[vote_id]['NO']:
                        print(f"    Noes: Summary={vote_details[vote_id]['noes']}, Detailed={detailed_vote_counts[vote_id]['NO']}")
                    if vote_details[vote_id]['abstentions'] != detailed_vote_counts[vote_id]['ABSTAIN']:
                        print(f"    Abstentions: Summary={vote_details[vote_id]['abstentions']}, Detailed={detailed_vote_counts[vote_id]['ABSTAIN']}")
                    if vote_details[vote_id]['excused'] != detailed_vote_counts[vote_id]['EXCUSED']:
                        print(f"    Excused: Summary={vote_details[vote_id]['excused']}, Detailed={detailed_vote_counts[vote_id]['EXCUSED']}")
                    if vote_details[vote_id]['recused'] != detailed_vote_counts[vote_id]['RECUSED']:
                        print(f"    Recused: Summary={vote_details[vote_id]['recused']}, Detailed={detailed_vote_counts[vote_id]['RECUSED']}")
                    if vote_details[vote_id]['non_voting'] != detailed_vote_counts[vote_id]['NON_VOTING']:
                        print(f"    Non-voting: Summary={vote_details[vote_id]['non_voting']}, Detailed={detailed_vote_counts[vote_id]['NON_VOTING']}")
        
        print(f"\nVote count validation (non-unanimous votes only):")
        print(f"Total non-unanimous votes in summary file: {total_non_unanimous_in_summary}")
        print(f"Total non-unanimous votes in detailed file: {total_non_unanimous_in_detailed}")
        
        if total_non_unanimous_in_summary == total_non_unanimous_in_detailed:
            print("✓ Non-unanimous vote counts match!")
        else:
            print("⚠ Warning: Non-unanimous vote counts don't match!")
            print(f"   Difference: {abs(total_non_unanimous_in_summary - total_non_unanimous_in_detailed)} votes")
            
            # Print detailed breakdown of non-unanimous votes for debugging
            print("\nDetailed breakdown of non-unanimous votes:")
            vote_types = {}
            for vote in non_unanimous_votes:
                vote_type = vote['vote_type']
                vote_types[vote_type] = vote_types.get(vote_type, 0) + 1
            for vote_type, count in sorted(vote_types.items()):
                print(f"{vote_type}: {count} votes")
    else:
        print("No vote records found")

if __name__ == "__main__":
    # Test with a single recent Common Council PDF
    test_pdf = "downloaded_minutes/COMMON_COUNCIL/2025-05-06.pdf"
    process_single_pdf(test_pdf) 