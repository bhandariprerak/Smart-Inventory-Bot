#!/usr/bin/env python3

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("=== Environment Check ===")
print("Current directory:", os.getcwd())
print("GOOGLE_SERVICE_ACCOUNT_FILE:", os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE'))
print("GOOGLE_DRIVE_FOLDER_ID:", os.getenv('GOOGLE_DRIVE_FOLDER_ID'))

service_account_file = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE')
print("File exists:", os.path.exists(service_account_file))

print("\n=== Testing Google Drive Service ===")
try:
    from app.services.google_drive import GoogleDriveService
    print("GoogleDriveService imported successfully")
    
    print("Creating Google Drive service...")
    drive = GoogleDriveService()
    print("Service created successfully")
    
    print("Listing files in folder...")
    files = drive.list_files_in_folder()
    print(f"Found {len(files)} files")
    
    for i, f in enumerate(files):
        print(f"  {i+1}. {f['name']} (ID: {f['id']})")
    
    if len(files) > 0:
        print("\nTesting CSV download...")
        csv_files = [f for f in files if f['name'].endswith('.csv')]
        if csv_files:
            test_file = csv_files[0]
            print(f"Downloading: {test_file['name']}")
            df = drive.download_csv_file(test_file['id'], test_file['name'])
            if df is not None:
                print(f"Successfully downloaded: {len(df)} rows, {len(df.columns)} columns")
                print("Columns:", list(df.columns))
            else:
                print("Failed to download CSV")
        else:
            print("No CSV files found")
    
    print("\n=== Testing CSV Processor ===")
    from app.services.csv_processor import CSVProcessor
    print("CSVProcessor imported successfully")
    
    processor = CSVProcessor()
    print("CSVProcessor created successfully")
    
    stats = processor.get_statistics()
    print("Statistics:", stats)
    
except Exception as e:
    print(f"ERROR: {str(e)}")
    import traceback
    traceback.print_exc()