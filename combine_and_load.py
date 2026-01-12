import os
from pathlib import Path
import pandas as pd
import duckdb


def load_alders_to_db():
    """Load alders dimension table from alders.csv into DuckDB."""
    alders_file = Path("alders.csv")
    if not alders_file.exists():
        print("No alders.csv found - run fetch_alders.py first")
        return False

    print("Loading alders dimension table...")
    alders_df = pd.read_csv(alders_file)

    db = duckdb.connect('madison_votes.db')
    try:
        db.execute("""
            CREATE OR REPLACE TABLE alders (
                person_id INTEGER,
                full_name VARCHAR,
                first_name VARCHAR,
                last_name VARCHAR,
                district INTEGER,
                member_type VARCHAR,
                start_date DATE,
                end_date DATE,
                email VARCHAR,
                extra_text VARCHAR,
                address VARCHAR,
                city VARCHAR,
                state VARCHAR,
                zip VARCHAR,
                phone VARCHAR,
                website VARCHAR
            )
        """)

        db.register('alders_df', alders_df)
        db.execute("""
            INSERT INTO alders
            SELECT
                person_id,
                full_name,
                first_name,
                last_name,
                district,
                member_type,
                CAST(start_date AS DATE),
                CAST(end_date AS DATE),
                email,
                extra_text,
                address,
                city,
                state,
                zip,
                phone,
                website
            FROM alders_df
        """)

        alder_count = db.execute("SELECT COUNT(*) FROM alders").fetchone()[0]
        print(f"Loaded {alder_count} alder records")

        # Create a view for current alders
        db.execute("""
            CREATE OR REPLACE VIEW current_alders AS
            SELECT *
            FROM alders
            WHERE end_date >= CURRENT_DATE
            ORDER BY district
        """)

        current_count = db.execute("SELECT COUNT(*) FROM current_alders").fetchone()[0]
        print(f"Current alders: {current_count}")

        # Create a view joining votes with alder info
        db.execute("""
            CREATE OR REPLACE VIEW votes_with_alder_info AS
            SELECT
                v.meeting_date,
                v.item_number,
                v.motion_number,
                v.motion_type,
                v.legistar_number,
                v.member_name,
                v.vote_type,
                v.is_unanimous,
                a.district,
                a.email,
                a.start_date as term_start,
                a.end_date as term_end
            FROM votes_by_member v
            LEFT JOIN alders a
                ON v.member_name = a.full_name
                AND v.meeting_date BETWEEN a.start_date AND a.end_date
            WHERE v.member_name != 'ALL_PRESENT'
            ORDER BY v.meeting_date DESC, a.district
        """)

        print("Created votes_with_alder_info view")

        # Load alder committees if available
        committees_file = Path("alder_committees.csv")
        if committees_file.exists():
            print("\nLoading alder committees...")
            committees_df = pd.read_csv(committees_file)

            db.execute("""
                CREATE OR REPLACE TABLE alder_committees (
                    person_id INTEGER,
                    body_id INTEGER,
                    body_name VARCHAR,
                    member_type VARCHAR,
                    title VARCHAR,
                    start_date DATE,
                    end_date DATE
                )
            """)

            db.register('committees_df', committees_df)
            db.execute("""
                INSERT INTO alder_committees
                SELECT
                    person_id,
                    body_id,
                    body_name,
                    member_type,
                    title,
                    CAST(start_date AS DATE),
                    CAST(end_date AS DATE)
                FROM committees_df
            """)

            committee_count = db.execute("SELECT COUNT(*) FROM alder_committees").fetchone()[0]
            print(f"Loaded {committee_count} committee membership records")

            # Create a view for current committee assignments
            db.execute("""
                CREATE OR REPLACE VIEW current_committee_assignments AS
                SELECT
                    a.full_name,
                    a.district,
                    c.body_name,
                    c.member_type,
                    c.title,
                    c.start_date,
                    c.end_date
                FROM alder_committees c
                JOIN alders a ON c.person_id = a.person_id
                WHERE c.end_date >= CURRENT_DATE
                  AND a.end_date >= CURRENT_DATE
                ORDER BY a.district, c.body_name
            """)

            print("Created current_committee_assignments view")

        return True

    except Exception as e:
        print(f"Error loading alders data: {e}")
        return False
    finally:
        db.close()


def combine_and_load_to_db():
    """Combine all CSV files and load them into DuckDB."""
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
            df = pd.read_csv(file)
            df['meeting_date'] = pd.to_datetime(date).strftime('%Y-%m-%d')  # Convert to string format
            summary_dfs.append(df)
        except Exception as e:
            print(f"Error processing summary file {file}: {e}")
            continue
    
    # Process detailed files
    for file in detailed_files:
        try:
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
        print("\nNo detailed data to combine")
        return
    
    # Initialize DuckDB connection
    print("\nInitializing DuckDB connection...")
    db = duckdb.connect('madison_votes.db')
    
    try:
        # Create tables
        print("Creating tables in DuckDB...")
        
        # Create and load summary table
        db.execute("""
            CREATE TABLE IF NOT EXISTS votes_summary (
                meeting_date DATE,
                item_number VARCHAR,
                motion_number VARCHAR,
                motion_title VARCHAR,
                motion_type VARCHAR,
                legistar_number VARCHAR,
                legistar_link VARCHAR,
                description TEXT,
                is_unanimous BOOLEAN,
                total_ayes INTEGER,
                total_noes INTEGER,
                total_abstentions INTEGER,
                total_excused INTEGER,
                total_recused INTEGER,
                total_non_voting INTEGER,
                page_number INTEGER
            )
        """)
        
        # Create and load detailed table
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
        print("Loading data into DuckDB...")
        db.register('summary_df', combined_summary)
        db.register('detailed_df', combined_detailed)
        
        # Use explicit CAST to DATE in the INSERT statements
        db.execute("""
            INSERT INTO votes_summary 
            SELECT 
                CAST(meeting_date AS DATE),
                item_number,
                motion_number,
                motion_title,
                motion_type,
                legistar_number,
                legistar_link,
                description,
                is_unanimous,
                total_ayes,
                total_noes,
                total_abstentions,
                total_excused,
                total_recused,
                total_non_voting,
                page_number
            FROM summary_df
        """)
        
        db.execute("""
            INSERT INTO votes_by_member 
            SELECT 
                CAST(meeting_date AS DATE),
                item_number,
                motion_number,
                motion_type,
                legistar_number,
                member_name,
                vote_type,
                is_unanimous
            FROM detailed_df
        """)
        
        # Create some useful views
        print("Creating views...")
        
        # View for non-unanimous votes
        db.execute("""
            CREATE OR REPLACE VIEW non_unanimous_votes AS
            SELECT 
                meeting_date,
                item_number,
                motion_number,
                motion_title,
                total_ayes,
                total_noes,
                total_abstentions,
                total_excused,
                total_recused,
                total_non_voting
            FROM votes_summary
            WHERE NOT is_unanimous
            ORDER BY meeting_date DESC, item_number::INTEGER, motion_number::INTEGER
        """)
        
        # View for member voting patterns
        db.execute("""
            CREATE OR REPLACE VIEW member_voting_patterns AS
            SELECT 
                member_name,
                vote_type,
                COUNT(*) as vote_count,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY member_name) as vote_percentage
            FROM votes_by_member
            WHERE NOT is_unanimous
            GROUP BY member_name, vote_type
            ORDER BY member_name, vote_type
        """)
        
        # View for votes with concatenated voter names
        db.execute("""
            CREATE OR REPLACE VIEW votes_with_voters AS
            SELECT 
                s.*,
                STRING_AGG(CASE WHEN d.vote_type = 'AYE' THEN d.member_name END, '; ') 
                    FILTER (WHERE d.vote_type = 'AYE') as ayes_list,
                STRING_AGG(CASE WHEN d.vote_type = 'NO' THEN d.member_name END, '; ') 
                    FILTER (WHERE d.vote_type = 'NO') as noes_list,
                STRING_AGG(CASE WHEN d.vote_type = 'ABSTAIN' THEN d.member_name END, '; ') 
                    FILTER (WHERE d.vote_type = 'ABSTAIN') as abstentions_list,
                STRING_AGG(CASE WHEN d.vote_type = 'EXCUSED' THEN d.member_name END, '; ') 
                    FILTER (WHERE d.vote_type = 'EXCUSED') as excused_list,
                STRING_AGG(CASE WHEN d.vote_type = 'RECUSED' THEN d.member_name END, '; ') 
                    FILTER (WHERE d.vote_type = 'RECUSED') as recused_list,
                STRING_AGG(CASE WHEN d.vote_type = 'NON_VOTING' THEN d.member_name END, '; ') 
                    FILTER (WHERE d.vote_type = 'NON_VOTING') as non_voting_list
            FROM votes_summary s
            LEFT JOIN votes_by_member d 
                ON s.meeting_date = d.meeting_date 
                AND s.item_number = d.item_number 
                AND s.motion_number = d.motion_number
            GROUP BY 
                s.meeting_date, s.item_number, s.motion_number, s.motion_title, 
                s.motion_type, s.legistar_number, s.legistar_link, s.description, 
                s.is_unanimous, s.total_ayes, s.total_noes, s.total_abstentions, 
                s.total_excused, s.total_recused, s.total_non_voting, s.page_number
            ORDER BY s.meeting_date DESC, s.item_number::INTEGER, s.motion_number::INTEGER
        """)
        
        print("\nSuccessfully loaded data into DuckDB!")
        
        # Print some summary statistics
        print("\nSummary Statistics:")
        print("-" * 50)
        
        total_meetings = db.execute("""
            SELECT COUNT(DISTINCT meeting_date) as meeting_count 
            FROM votes_summary
        """).fetchone()[0]
        print(f"Total meetings processed: {total_meetings}")
        
        total_votes = db.execute("""
            SELECT COUNT(*) as vote_count 
            FROM votes_summary
        """).fetchone()[0]
        print(f"Total votes recorded: {total_votes}")
        
        non_unanimous = db.execute("""
            SELECT COUNT(*) as non_unanimous_count 
            FROM votes_summary 
            WHERE NOT is_unanimous
        """).fetchone()[0]
        print(f"Non-unanimous votes: {non_unanimous}")
        
        print("\nMost common vote types:")
        print(db.execute("""
            SELECT vote_type, COUNT(*) as count
            FROM votes_by_member
            GROUP BY vote_type
            ORDER BY count DESC
        """).fetch_df().to_string())

        # Load alders dimension table if available
        print("\n" + "-" * 50)
        db.close()
        load_alders_to_db()
        return

    except Exception as e:
        print(f"Error loading data into DuckDB: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--alders-only":
        load_alders_to_db()
    else:
        combine_and_load_to_db()