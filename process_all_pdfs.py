import os
from pathlib import Path
from datetime import datetime
from extract_votes import process_single_pdf

def process_all_pdfs(start_date: str = None, end_date: str = None):
    """
    Process PDFs in the downloaded_minutes/COMMON_COUNCIL directory within a date range.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format. If None, process all files from the beginning.
        end_date (str): End date in YYYY-MM-DD format. If None, process all files up to the most recent.
    """
    # Get the directory containing the PDFs
    pdf_dir = Path("downloaded_minutes/COMMON_COUNCIL")
    
    # Get all PDF files
    pdf_files = sorted([f for f in pdf_dir.glob("*.pdf")])
    
    # Filter files by date if date range is specified
    if start_date or end_date:
        filtered_files = []
        for pdf_file in pdf_files:
            # Extract date from filename (format: YYYY-MM-DD.pdf)
            file_date = pdf_file.stem
            try:
                file_date = datetime.strptime(file_date, "%Y-%m-%d")
                
                # Apply date filters
                if start_date and file_date < datetime.strptime(start_date, "%Y-%m-%d"):
                    continue
                if end_date and file_date > datetime.strptime(end_date, "%Y-%m-%d"):
                    continue
                    
                filtered_files.append(pdf_file)
            except ValueError:
                print(f"Warning: Could not parse date from filename {pdf_file}")
                continue
        pdf_files = filtered_files
    
    print(f"\nFound {len(pdf_files)} PDF files to process")
    
    # Process each PDF
    for i, pdf_file in enumerate(pdf_files, 1):
        print(f"\n{'='*80}")
        print(f"Processing PDF {i}/{len(pdf_files)}: {pdf_file}")
        print('='*80)
        
        try:
            # Auto-detect unapproved minutes (pass None to enable auto-detection)
            process_single_pdf(str(pdf_file), unapproved_minutes=None)
        except Exception as e:
            print(f"Error processing {pdf_file}: {e}")
            continue
        
        print(f"\nCompleted processing {pdf_file}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Process Common Council meeting minutes PDFs")
    parser.add_argument("--start-date", help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", help="End date in YYYY-MM-DD format")
    parser.add_argument("--single-file", help="Process a single file by date (YYYY-MM-DD)")
    args = parser.parse_args()
    
    if args.single_file:
        # Process just the single file
        pdf_file = Path("downloaded_minutes/COMMON_COUNCIL") / f"{args.single_file}.pdf"
        if pdf_file.exists():
            print(f"\nProcessing single file: {pdf_file}")
            try:
                # Auto-detect unapproved minutes (pass None to enable auto-detection)
                process_single_pdf(str(pdf_file), unapproved_minutes=None)
                print(f"\nCompleted processing {pdf_file}")
            except Exception as e:
                print(f"Error processing {pdf_file}: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"File not found: {pdf_file}")
    else:
        process_all_pdfs(start_date=args.start_date, end_date=args.end_date) 