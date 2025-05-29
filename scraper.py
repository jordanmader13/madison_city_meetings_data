import os
import requests
from datetime import datetime
from tqdm import tqdm
import json
import argparse

class MadisonLegistarScraper:
    def __init__(self):
        self.base_url = "https://webapi.legistar.com/v1/madison"
        self.events_url = f"{self.base_url}/events"
        self.output_dir = "downloaded_minutes"
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def fetch_events(self, start_date=None, end_date=None):
        """
        Fetch events from the Legistar API with optional date filtering
        """
        params = {}
        if start_date:
            params['$filter'] = f"EventDate ge datetime'{start_date}'"
            if end_date:
                params['$filter'] += f" and EventDate le datetime'{end_date}'"
        
        try:
            response = requests.get(self.events_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching events: {e}")
            return None
    
    def download_minutes(self, event):
        """
        Download minutes file for a given event if available
        """
        if not event.get('EventMinutesFile'):
            return False
        
        file_url = event['EventMinutesFile']
        event_date = datetime.strptime(event['EventDate'], "%Y-%m-%dT%H:%M:%S")
        committee_name = event['EventBodyName'].replace('/', '_').replace('\\', '_')
        
        filename = f"{event_date.strftime('%Y-%m-%d')}_{committee_name}_minutes.pdf"
        filepath = os.path.join(self.output_dir, filename)
        
        # Skip if file already exists
        if os.path.exists(filepath):
            print(f"Skipping existing file: {filename}")
            return True
        
        try:
            response = requests.get(file_url, stream=True)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return True
        except requests.exceptions.RequestException as e:
            print(f"Error downloading minutes for event {event['EventId']}: {e}")
            return False

def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        msg = f"Not a valid date: '{s}'. Use YYYY-MM-DD format."
        raise argparse.ArgumentTypeError(msg)

def main():
    parser = argparse.ArgumentParser(description='Download Madison Legistar meeting minutes.')
    parser.add_argument('--start-date', type=valid_date,
                      default="2024-01-01",
                      help='Start date in YYYY-MM-DD format (default: 2024-01-01)')
    parser.add_argument('--end-date', type=valid_date,
                      default=datetime.now().strftime("%Y-%m-%d"),
                      help='End date in YYYY-MM-DD format (default: today)')
    
    args = parser.parse_args()
    
    print(f"Fetching minutes from {args.start_date} to {args.end_date}")
    
    scraper = MadisonLegistarScraper()
    events = scraper.fetch_events(start_date=args.start_date, end_date=args.end_date)
    
    if not events:
        print("No events found or error occurred")
        return
    
    print(f"Found {len(events)} events")
    
    # Download minutes for events that have them
    successful_downloads = 0
    for event in tqdm(events, desc="Downloading minutes"):
        if scraper.download_minutes(event):
            successful_downloads += 1
    
    print(f"\nSuccessfully downloaded {successful_downloads} minute files")

if __name__ == "__main__":
    main() 