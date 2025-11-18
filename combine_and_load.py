import os
from pathlib import Path
import pandas as pd
import duckdb
from datetime import datetime

def combine_and_load_to_db(start_date: str = None, end_date: str = None):
    """
    Combine CSV files and load them into DuckDB within a date range.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format. If None, process all files from the beginning.
        end_date (str): End date in YYYY-MM-DD format. If None, process all files up to the most recent.
    """
    # Get the directory containing the CSVs
    csv_dir = Path("downloaded_minutes/COMMON_COUNCIL")
    
    # Initialize lists to store dataframes
    summary_dfs = []
    detailed_dfs = []
    
    # Get all CSV files
    summary_files = sorted([f for f in csv_dir.glob("*_votes_summary.csv")])
    detailed_files = sorted([f for f in csv_dir.glob("*_votes_detailed.csv")])
    
    print(f"\nFound {len(summary_files)} summary files and {len(detailed_files)} detailed files")
    
    # Process summary files
    for file in summary_files:
        try:
            # Extract date from filename (format: YYYY-MM-DD_votes_summary.csv)
            date = file.stem.split('_')[0]
            
            # Apply date filters
            if start_date and date < start_date:
                continue
            if end_date and date > end_date:
                continue
                
            df = pd.read_csv(file)
            df['meeting_date'] = pd.to_datetime(date).strftime('%Y-%m-%d')  # Convert to string format
            summary_dfs.append(df)
        except Exception as e:
            print(f"Error processing summary file {file}: {e}")
            continue
    
    # Process detailed files
    for file in detailed_files:
        try:
            # Extract date from filename (format: YYYY-MM-DD_votes_detailed.csv)
            date = file.stem.split('_')[0]
            
            # Apply date filters
            if start_date and date < start_date:
                continue
            if end_date and date > end_date:
                continue
                
            df = pd.read_csv(file)
            # Date is already in the detailed files
            df['meeting_date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')  # Convert to string format
            detailed_dfs.append(df)
        except Exception as e:
            print(f"Error processing detailed file {file}: {e}")
            continue
    
    # Combine all dataframes
    if summary_dfs:
        combined_summary = pd.concat(summary_dfs, ignore_index=True)
        print(f"\nCombined summary data shape: {combined_summary.shape}")
    else:
        print("\nNo summary data to combine")
        return
        
    if detailed_dfs:
        combined_detailed = pd.concat(detailed_dfs, ignore_index=True)
        print(f"Combined detailed data shape: {combined_detailed.shape}")
    else:
        print("No detailed data to combine")
        return
    
    # Connect to DuckDB
    db = duckdb.connect('madison_votes.db')
    
    # Create tables
    db.execute("""
        CREATE TABLE IF NOT EXISTS votes_summary (
            item_number VARCHAR,
            motion_number VARCHAR,
            motion_title VARCHAR,
            motion_type VARCHAR,
            legistar_number VARCHAR,
            legistar_link VARCHAR,
            description VARCHAR,
            is_unanimous BOOLEAN,
            total_ayes INTEGER,
            total_noes INTEGER,
            total_abstentions INTEGER,
            total_excused INTEGER,
            total_recused INTEGER,
            total_non_voting INTEGER,
            page_number INTEGER,
            meeting_date DATE
        )
    """)
    
    db.execute("""
        CREATE TABLE IF NOT EXISTS votes_by_member (
            meeting_date DATE,
            item_number VARCHAR,
            motion_number VARCHAR,
            motion_type VARCHAR,
            legistar_number VARCHAR,
            member_name VARCHAR,
            vote_type VARCHAR,
            is_unanimous BOOLEAN
        )
    """)
    
    # Load data into tables
    db.execute("DELETE FROM votes_summary")
    db.execute("DELETE FROM votes_by_member")
    
    db.execute("INSERT INTO votes_summary SELECT * FROM combined_summary")
    db.execute("INSERT INTO votes_by_member SELECT * FROM combined_detailed")
    
    # Create views
    db.execute("""
        CREATE OR REPLACE VIEW votes_with_voters AS
        SELECT 
            vs.*,
            GROUP_CONCAT(CASE WHEN vbm.vote_type = 'AYE' THEN vbm.member_name END) as aye_voters,
            GROUP_CONCAT(CASE WHEN vbm.vote_type = 'NO' THEN vbm.member_name END) as no_voters,
            GROUP_CONCAT(CASE WHEN vbm.vote_type = 'ABSTAIN' THEN vbm.member_name END) as abstain_voters,
            GROUP_CONCAT(CASE WHEN vbm.vote_type = 'EXCUSED' THEN vbm.member_name END) as excused_voters,
            GROUP_CONCAT(CASE WHEN vbm.vote_type = 'RECUSED' THEN vbm.member_name END) as recused_voters,
            GROUP_CONCAT(CASE WHEN vbm.vote_type = 'NON_VOTING' THEN vbm.member_name END) as non_voting_voters
        FROM votes_summary vs
        LEFT JOIN votes_by_member vbm 
            ON vs.meeting_date = vbm.meeting_date 
            AND vs.item_number = vbm.item_number 
            AND vs.motion_number = vbm.motion_number
        GROUP BY vs.meeting_date, vs.item_number, vs.motion_number
    """)
    
    db.execute("""
        CREATE OR REPLACE VIEW non_unanimous_votes AS
        SELECT * FROM votes_with_voters
        WHERE NOT is_unanimous
        ORDER BY meeting_date DESC
    """)
    
    db.execute("""
        CREATE OR REPLACE VIEW member_voting_patterns AS
        SELECT 
            member_name,
            COUNT(*) as total_votes,
            SUM(CASE WHEN vote_type = 'AYE' THEN 1 ELSE 0 END) as aye_votes,
            SUM(CASE WHEN vote_type = 'NO' THEN 1 ELSE 0 END) as no_votes,
            SUM(CASE WHEN vote_type = 'ABSTAIN' THEN 1 ELSE 0 END) as abstain_votes,
            SUM(CASE WHEN vote_type = 'EXCUSED' THEN 1 ELSE 0 END) as excused_votes,
            SUM(CASE WHEN vote_type = 'RECUSED' THEN 1 ELSE 0 END) as recused_votes,
            SUM(CASE WHEN vote_type = 'NON_VOTING' THEN 1 ELSE 0 END) as non_voting_votes
        FROM votes_by_member
        GROUP BY member_name
        ORDER BY total_votes DESC
    """)
    
    print("\nDatabase updated successfully!")
    db.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Combine CSV files and load into DuckDB")
    parser.add_argument("--start-date", help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", help="End date in YYYY-MM-DD format")
    args = parser.parse_args()
    
    combine_and_load_to_db(start_date=args.start_date, end_date=args.end_date) 