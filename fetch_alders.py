import re
import requests
import pandas as pd
from pathlib import Path

BASE_URL = "https://webapi.legistar.com/v1/madison"
COMMON_COUNCIL_BODY_ID = 1


def extract_district(email, sort_value):
    """Extract district number from email or sort value."""
    # Try email first (e.g., district3@cityofmadison.com)
    if email:
        match = re.search(r'district(\d+)@', email)
        if match:
            return int(match.group(1))

    # Fall back to sort value (e.g., 301 -> district 3)
    if sort_value:
        return sort_value // 100

    return None

def fetch_office_records():
    """Fetch all office records for the Common Council."""
    url = f"{BASE_URL}/bodies/{COMMON_COUNCIL_BODY_ID}/OfficeRecords"

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching office records: {e}")
        return None

def fetch_persons():
    """Fetch all persons from the API for additional contact details."""
    url = f"{BASE_URL}/persons"

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching persons: {e}")
        return None


def fetch_person_office_records(person_id):
    """Fetch all office records (committee memberships) for a specific person."""
    url = f"{BASE_URL}/persons/{person_id}/OfficeRecords"

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching office records for person {person_id}: {e}")
        return None

def process_alders():
    """Fetch and process alder data into a clean dataframe."""
    print("Fetching Common Council office records...")
    office_records = fetch_office_records()

    if not office_records:
        print("No office records found")
        return None

    print(f"Found {len(office_records)} office records")

    print("Fetching person details...")
    persons = fetch_persons()
    persons_dict = {}
    if persons:
        persons_dict = {p['PersonId']: p for p in persons}
        print(f"Found {len(persons)} person records")

    # Process into a clean dataframe
    records = []
    for rec in office_records:
        person_id = rec.get('OfficeRecordPersonId')
        person = persons_dict.get(person_id, {})
        email = rec.get('OfficeRecordEmail') or person.get('PersonEmail')
        sort_value = rec.get('OfficeRecordSort')

        records.append({
            'person_id': person_id,
            'full_name': rec.get('OfficeRecordFullName'),
            'first_name': rec.get('OfficeRecordFirstName'),
            'last_name': rec.get('OfficeRecordLastName'),
            'district': extract_district(email, sort_value),
            'member_type': rec.get('OfficeRecordMemberType'),
            'start_date': rec.get('OfficeRecordStartDate'),
            'end_date': rec.get('OfficeRecordEndDate'),
            'email': email,
            'extra_text': rec.get('OfficeRecordExtraText'),
            # Additional person details
            'address': person.get('PersonAddress1'),
            'city': person.get('PersonCity1'),
            'state': person.get('PersonState1'),
            'zip': person.get('PersonZip1'),
            'phone': person.get('PersonPhone'),
            'website': person.get('PersonWWW'),
        })

    df = pd.DataFrame(records)

    # Clean up dates
    df['start_date'] = pd.to_datetime(df['start_date']).dt.date
    df['end_date'] = pd.to_datetime(df['end_date']).dt.date

    return df

def process_committees(alders_df):
    """Fetch committee memberships for all alders."""
    # Get unique person IDs from alders
    person_ids = alders_df['person_id'].unique()
    print(f"\nFetching committee memberships for {len(person_ids)} persons...")

    records = []
    for i, person_id in enumerate(person_ids):
        if (i + 1) % 20 == 0:
            print(f"  Progress: {i + 1}/{len(person_ids)}")

        office_records = fetch_person_office_records(person_id)
        if not office_records:
            continue

        for rec in office_records:
            # Skip Common Council itself (we already have that in alders table)
            if rec.get('OfficeRecordBodyId') == COMMON_COUNCIL_BODY_ID:
                continue

            records.append({
                'person_id': person_id,
                'body_id': rec.get('OfficeRecordBodyId'),
                'body_name': rec.get('OfficeRecordBodyName'),
                'member_type': rec.get('OfficeRecordMemberType'),
                'title': rec.get('OfficeRecordTitle'),
                'start_date': rec.get('OfficeRecordStartDate'),
                'end_date': rec.get('OfficeRecordEndDate'),
            })

    if not records:
        return None

    df = pd.DataFrame(records)

    # Clean up dates (handle out-of-bounds dates like 9999-12-31)
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce').dt.date
    df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce').dt.date

    return df


def main():
    df = process_alders()

    if df is None:
        return

    # Save to CSV
    output_path = Path("alders.csv")
    df.to_csv(output_path, index=False)
    print(f"\nSaved {len(df)} alder records to {output_path}")

    # Print summary
    print("\nSummary:")
    print("-" * 50)
    print(f"Total records: {len(df)}")
    print(f"Unique persons: {df['person_id'].nunique()}")
    print(f"Date range: {df['start_date'].min()} to {df['end_date'].max()}")

    print("\nMember types:")
    print(df['member_type'].value_counts().to_string())

    print("\nSample of current alders (end_date >= 2025):")
    current = df[df['end_date'] >= pd.to_datetime('2025-01-01').date()].sort_values('district')
    print(current[['full_name', 'district', 'start_date', 'end_date']].to_string())

    # Fetch and save committee memberships
    committees_df = process_committees(df)

    if committees_df is not None:
        committees_path = Path("alder_committees.csv")
        committees_df.to_csv(committees_path, index=False)
        print(f"\nSaved {len(committees_df)} committee membership records to {committees_path}")

        print("\nCommittee summary:")
        print(f"Unique committees: {committees_df['body_name'].nunique()}")
        print(f"Alders with committee assignments: {committees_df['person_id'].nunique()}")

        print("\nTop 10 committees by membership count:")
        print(committees_df['body_name'].value_counts().head(10).to_string())


if __name__ == "__main__":
    main()
