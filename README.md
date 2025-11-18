# Madison WI City Meetings Vote Tracker

This project extracts, processes, and summarizes voting records from Madison City meeting minutes. It produces a database of votes, including who voted and how they voted on each item.

This project was heavily written by Cursor agent + claude-3.5-sonnet. It has undergone some quality checking but there are still some loose ends to tie up. I welcome data problems reported as Github issues. 

The repo currently contains a DuckDB database file containing voting records for Common Council meetings from 2023 through the May 6, 2025 meeting. The other elements of the processing pipeline are not pushed to Github at the moment. 

## Processing Pipeline

The project consists of several scripts that work in a sequence:

1. `scraper.py`: Downloads meeting minutes PDFs from Madison's Legistar system
   - Fetches PDFs of all city meetings in Legistar
   - Saves them to `downloaded_minutes/` directory

2. `organize_minutes.py`: Organizes the downloaded PDFs
   - Replaces spaces with underscores in filenames
   - Creates meeting-type folders
   - Moves PDFs to appropriate folders with date-based names
   - End result here is folders with all city committees with PDFs of meeting minutes.

3. `extract_votes.py`: Processes a single PDF to extract voting data
   - Parses PDF text to find vote records
   - Extracts vote counts, member names, and motion details
   - Creates summary and detailed CSV files for each meeting
   - This is optimized for Common Council at the moment. It's possible the structure generalizes but I haven't tried yet.

4. `process_all_pdfs.py`: Batch processes multiple PDFs
   - Loops through all PDFs in the Common Council folder
   - Calls `extract_votes.py` for each PDF
   - Handles errors and provides progress updates
   - Supports date range filtering with `--start-date` and `--end-date` arguments

5. `combine_and_load.py`: Creates and populates the database
   - Combines all CSV files from processed PDFs
   - Creates DuckDB database with tables and views
   - Loads all voting data into the database
   - Supports date range filtering with `--start-date` and `--end-date` arguments

6. `query_votes.py`: Streamlit app to query the database
   - Simple query editor to write queries against the database
   - This is just a simple way to interact with the DuckDB rather than interactively in a python console or something

## Database Structure

The data is stored in a DuckDB database (`madison_votes.db`) with the following structure:

### Tables
- `votes_summary`: Contains summary information for each vote
- `votes_by_member`: Contains individual voting records for each council member

### Views
- `votes_with_voters`: Combines vote summaries with lists of who voted which way
- `non_unanimous_votes`: Shows all non-unanimous votes with vote counts
- `member_voting_patterns`: Summary of votes by council member

## Data Format

Each vote record includes:
- Meeting date
- Item number and motion number
- Motion title and type
- Legistar reference number and link
- Vote counts (ayes, noes, abstentions, etc.)
- Lists of council members for each vote type (aye, no, abstain, etc.)

## Usage

### Processing New Minutes

To process new meeting minutes:

```bash
# 1. Download new PDFs
python scraper.py

# 2. Organize the PDFs
python organize_minutes.py

# 3. Process all PDFs (optionally with date range)
python process_all_pdfs.py [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]

# 4. Update the database (optionally with date range)
python combine_and_load.py [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]
```

### Querying the Data

1. Launch the Streamlit app:
```bash
streamlit run query_votes.py
```

2. Or query the database directly:
```python
import duckdb
db = duckdb.connect('madison_votes.db')
result = db.execute("""
    SELECT * FROM votes_with_voters 
    WHERE NOT is_unanimous 
    ORDER BY meeting_date DESC
""").fetchdf()
```

## Data Sources

The data is sourced from Madison's Legistar system, which contains official meeting minutes and voting records for all city meetings. The PDFs are downloaded from the Legistar website and processed to extract structured voting data.

## Note

PDF and CSV files are not included in the repository to keep it lightweight. The processed data is available in the DuckDB database file.

## Unapproved Minutes

The script includes automatic detection and handling for unapproved (non-finalized) meeting minutes, which have a different format with narrative text like "A motion was made by X, seconded by Y...". This functionality is based on a single example and may need refinement as more unapproved minutes are encountered. 