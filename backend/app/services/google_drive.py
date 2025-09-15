import os
import io
import pandas as pd
from typing import Dict, Optional
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class GoogleDriveService:
    def __init__(self):
        self.service = None
        self.folder_id = os.getenv('GOOGLE_DRIVE_FOLDER_ID')
        self.service_account_file = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE')
        self.csv_cache = {}  # Cache for CSV data
        self._initialize_service()
    
    def _initialize_service(self):
        """Initialize Google Drive service with service account credentials"""
        try:
            if not os.path.exists(self.service_account_file):
                raise FileNotFoundError(f"Service account file not found: {self.service_account_file}")
            
            # Set up credentials
            SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
            credentials = service_account.Credentials.from_service_account_file(
                self.service_account_file, scopes=SCOPES
            )
            
            # Build the service
            self.service = build('drive', 'v3', credentials=credentials)
            logger.info("Google Drive service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Google Drive service: {str(e)}")
            raise
    
    def list_files_in_folder(self) -> list:
        """List all files in the specified Google Drive folder"""
        try:
            query = f"'{self.folder_id}' in parents"
            results = self.service.files().list(
                q=query,
                fields="files(id, name, mimeType, modifiedTime)"
            ).execute()
            
            files = results.get('files', [])
            logger.info(f"Found {len(files)} files in Google Drive folder")
            return files
            
        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")
            return []
    
    def download_csv_file(self, file_id: str, file_name: str) -> Optional[pd.DataFrame]:
        """Download a CSV file from Google Drive and return as pandas DataFrame"""
        try:
            # Get file content
            request = self.service.files().get_media(fileId=file_id)
            file_content = io.BytesIO()
            downloader = MediaIoBaseDownload(file_content, request)
            
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            
            # Convert to pandas DataFrame
            file_content.seek(0)
            df = pd.read_csv(file_content)
            
            logger.info(f"Successfully downloaded {file_name}: {len(df)} rows, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Error downloading {file_name}: {str(e)}")
            return None
    
    def load_all_csv_files(self) -> Dict[str, pd.DataFrame]:
        """Load all CSV files from Google Drive folder"""
        try:
            files = self.list_files_in_folder()
            csv_files = [f for f in files if f['name'].endswith('.csv')]
            
            csv_data = {}
            for file_info in csv_files:
                file_name = file_info['name']
                file_id = file_info['id']
                
                # Download and cache the file
                df = self.download_csv_file(file_id, file_name)
                if df is not None:
                    # Remove .csv extension for key
                    key = file_name.replace('.csv', '').lower()
                    csv_data[key] = df
                    self.csv_cache[key] = df
            
            logger.info(f"Loaded {len(csv_data)} CSV files: {list(csv_data.keys())}")
            return csv_data
            
        except Exception as e:
            logger.error(f"Error loading CSV files: {str(e)}")
            return {}
    
    def get_csv_data(self, csv_name: str) -> Optional[pd.DataFrame]:
        """Get specific CSV data by name (customer, inventory, detail, pricelist)"""
        csv_name = csv_name.lower()
        
        # Check cache first
        if csv_name in self.csv_cache:
            return self.csv_cache[csv_name]
        
        # If not in cache, reload all files
        self.load_all_csv_files()
        return self.csv_cache.get(csv_name)
    
    def refresh_cache(self) -> Dict[str, pd.DataFrame]:
        """Refresh the CSV cache by reloading all files"""
        logger.info("Refreshing CSV cache...")
        self.csv_cache.clear()
        return self.load_all_csv_files()
    
    def get_file_info(self) -> dict:
        """Get information about all CSV files"""
        info = {}
        for name, df in self.csv_cache.items():
            if df is not None:
                info[name] = {
                    "rows": len(df),
                    "columns": len(df.columns),
                    "column_names": list(df.columns),
                    "memory_usage": f"{df.memory_usage(deep=True).sum() / 1024:.2f} KB"
                }
        return info