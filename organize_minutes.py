import os
import re
from pathlib import Path
from typing import Dict, List, Tuple

def organize_minutes(dry_run: bool = False) -> Tuple[Dict[str, int], List[str]]:
    """
    Organize meeting minutes PDFs by:
    1. Replacing spaces with underscores in all filenames
    2. Organizing files into meeting-type folders
    3. Renaming files to date-based format
    
    Args:
        dry_run: If True, only print what would be done without making changes
        
    Returns:
        Tuple of (statistics dict, list of errors)
    """
    minutes_dir = Path("downloaded_minutes")
    
    if not minutes_dir.exists():
        print(f"Error: {minutes_dir} directory not found")
        return {}, ["Minutes directory not found"]
    
    # Initialize tracking
    stats = {
        "files_found": 0,
        "files_renamed": 0,
        "files_organized": 0,
        "errors": 0
    }
    errors = []
    
    # Step 1: Replace spaces with underscores in root directory only
    print("\nStep 1: Replacing spaces with underscores in root directory...")
    for file_path in minutes_dir.glob("*"):
        if file_path.is_file() and " " in file_path.name:
            stats["files_found"] += 1
            try:
                new_name = file_path.name.replace(" ", "_")
                new_path = file_path.parent / new_name
                
                if not dry_run:
                    file_path.rename(new_path)
                print(f"Renamed: {file_path.name} -> {new_name}")
                stats["files_renamed"] += 1
            except Exception as e:
                error_msg = f"Error renaming {file_path.name}: {e}"
                print(error_msg)
                errors.append(error_msg)
                stats["errors"] += 1
    
    # Step 2: Organize files into meeting-type folders
    print("\nStep 2: Organizing files into meeting-type folders...")
    # Pattern to match date and meeting type: YYYY-MM-DD_MEETING_TYPE
    pattern = r"(\d{4}-\d{2}-\d{2})_(.*?)_minutes\.pdf"
    
    # Get all PDF files in the root minutes directory
    pdf_files = list(minutes_dir.glob("*.pdf"))
    stats["files_found"] = len(pdf_files)
    
    if not pdf_files:
        print("No PDF files found in the root directory")
        return stats, errors
    
    print(f"Found {len(pdf_files)} PDF files to organize")
    
    for pdf_path in pdf_files:
        try:
            match = re.match(pattern, pdf_path.name)
            if match:
                date, meeting_type = match.groups()
                
                # Clean up meeting type and create folder name
                meeting_folder = meeting_type.replace(' ', '_')
                folder_path = minutes_dir / meeting_folder
                
                # Create folder if it doesn't exist
                if not dry_run:
                    folder_path.mkdir(exist_ok=True)
                
                # Create new filename: YYYY-MM-DD.pdf
                new_filename = f"{date}.pdf"
                new_path = folder_path / new_filename
                
                # Move and rename file
                if not dry_run:
                    pdf_path.rename(new_path)
                print(f"Organized: {pdf_path.name} -> {meeting_folder}/{new_filename}")
                stats["files_organized"] += 1
            else:
                error_msg = f"Warning: Couldn't parse filename pattern for {pdf_path.name}"
                print(error_msg)
                errors.append(error_msg)
                stats["errors"] += 1
                
        except Exception as e:
            error_msg = f"Error processing {pdf_path.name}: {e}"
            print(error_msg)
            errors.append(error_msg)
            stats["errors"] += 1
    
    # Print summary
    print(f"\nSummary:")
    print(f"Files found: {stats['files_found']}")
    print(f"Files renamed (spaces removed): {stats['files_renamed']}")
    print(f"Files organized into folders: {stats['files_organized']}")
    print(f"Errors: {stats['errors']}")
    
    if dry_run:
        print("\nThis was a dry run - no files were actually modified")
    
    return stats, errors

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Organize meeting minutes PDFs")
    parser.add_argument("--dry-run", action="store_true", 
                      help="Print what would be done without making changes")
    args = parser.parse_args()
    
    organize_minutes(dry_run=args.dry_run) 