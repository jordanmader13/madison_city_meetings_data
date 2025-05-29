import os
from pathlib import Path
from extract_votes import process_single_pdf

def process_all_pdfs():
    """Process all PDFs in the downloaded_minutes/COMMON_COUNCIL directory."""
    # Get the directory containing the PDFs
    pdf_dir = Path("downloaded_minutes/COMMON_COUNCIL")
    
    # Get all PDF files
    pdf_files = sorted([f for f in pdf_dir.glob("*.pdf")])
    
    print(f"\nFound {len(pdf_files)} PDF files to process")
    
    # Process each PDF
    for i, pdf_file in enumerate(pdf_files, 1):
        print(f"\n{'='*80}")
        print(f"Processing PDF {i}/{len(pdf_files)}: {pdf_file}")
        print('='*80)
        
        try:
            process_single_pdf(str(pdf_file))
        except Exception as e:
            print(f"Error processing {pdf_file}: {e}")
            continue
        
        print(f"\nCompleted processing {pdf_file}")

if __name__ == "__main__":
    process_all_pdfs() 