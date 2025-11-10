import config
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import json
import gc
import os
from typing import Optional, List, Dict, Tuple

# Config
tenant_id = config.AZURE_TENANT_ID
client_id = config.AZURE_CLIENT_ID
client_secret = config.AZURE_CLIENT_SECRET
account_name = config.AZURE_ACCOUNT_NAME
container_name = config.AZURE_CONTAINER_NAME
source_path = config.AZURE_REMOTE_PATH  # e.g., "raw/"

# Configuration for filtering and moving files
# Filter files uploaded after this date (None = no date filter)
FILTER_DATE_AFTER: Optional[datetime] = None  # e.g., datetime(2024, 1, 1)
# Filter files uploaded before this date (None = no date filter)
FILTER_DATE_BEFORE: Optional[datetime] = None  # e.g., datetime(2024, 12, 31)
# SalesCompanyId to filter by (None = check all files but don't filter by this)
TARGET_SALES_COMPANY_ID: Optional[str] = None  # e.g., "12345"
# Target path where matching files will be moved
TARGET_PATH = "/files/sbt/quotes/"  # e.g., "archive/"

MAX_WORKERS = 16  # Tune this based on your machine/network

print(f"Source path: {source_path}")
print(f"Target path: {TARGET_PATH}")
if FILTER_DATE_AFTER:
    print(f"Filter files uploaded after: {FILTER_DATE_AFTER}")
if FILTER_DATE_BEFORE:
    print(f"Filter files uploaded before: {FILTER_DATE_BEFORE}")
if TARGET_SALES_COMPANY_ID:
    print(f"Filter by SalesCompanyId: {TARGET_SALES_COMPANY_ID}")

# Auth & Client Setup
credential = ClientSecretCredential(tenant_id, client_id, client_secret)
service_client = DataLakeServiceClient(
    account_url=f"https://{account_name}.dfs.core.windows.net",
    credential=credential
)
file_system_client = service_client.get_file_system_client(container_name)


def check_file_content(file_client, sales_company_id: Optional[str]) -> bool:
    """
    Download and check file content for SalesCompanyId.
    Returns True if file should be processed (matches criteria or no filter specified).
    """
    try:
        # Download file content
        download = file_client.download_file()
        content = download.readall()
        
        # Try to parse as JSON
        try:
            data = json.loads(content.decode('utf-8'))
            
            # If SalesCompanyId filter is specified, check for it
            if sales_company_id is not None:
                # Check various possible locations for SalesCompanyId
                sales_company_id_found = None
                
                # Direct key
                if 'SalesCompanyId' in data:
                    sales_company_id_found = str(data['SalesCompanyId'])
                # Nested in common structures
                elif isinstance(data, dict):
                    # Check nested objects
                    for key, value in data.items():
                        if isinstance(value, dict) and 'SalesCompanyId' in value:
                            sales_company_id_found = str(value['SalesCompanyId'])
                            break
                        elif isinstance(value, list):
                            # Check first item in list if it's a dict
                            if value and isinstance(value[0], dict) and 'SalesCompanyId' in value[0]:
                                sales_company_id_found = str(value[0]['SalesCompanyId'])
                                break
                
                # Compare with target (convert both to strings for comparison)
                if sales_company_id_found is None:
                    print(f"  Warning: SalesCompanyId not found in file content")
                    return False
                
                if str(sales_company_id_found) != str(sales_company_id):
                    print(f"  SalesCompanyId mismatch: found '{sales_company_id_found}', expected '{sales_company_id}'")
                    return False
                
                print(f"  ✓ SalesCompanyId matches: {sales_company_id_found}")
            
            return True
            
        except json.JSONDecodeError:
            print(f"  Warning: File is not valid JSON, skipping content check")
            # If not JSON and we need to check SalesCompanyId, skip it
            if sales_company_id is not None:
                return False
            return True
            
    except Exception as e:
        print(f"  Error checking file content: {e}")
        return False


def should_process_file(file_path: str, file_properties, date_after: Optional[datetime], 
                        date_before: Optional[datetime], sales_company_id: Optional[str]) -> Tuple[bool, str]:
    """
    Check if file should be processed based on upload date and content.
    Returns (should_process, reason)
    """
    try:
        # Get file client
        file_client = file_system_client.get_file_client(file_path)
        
        # Check upload date (creation_time or last_modified)
        file_date = None
        if hasattr(file_properties, 'creation_time') and file_properties.creation_time:
            file_date = file_properties.creation_time
        elif hasattr(file_properties, 'last_modified') and file_properties.last_modified:
            file_date = file_properties.last_modified
        
        if file_date:
            # Check date filters
            if date_after and file_date < date_after:
                return (False, f"File uploaded before filter date: {file_date}")
            
            if date_before and file_date > date_before:
                return (False, f"File uploaded after filter date: {file_date}")
            
            print(f"Checking file: {file_path} (uploaded: {file_date})")
        else:
            print(f"Checking file: {file_path} (upload date unknown)")
        
        # Check file content for SalesCompanyId if specified
        if sales_company_id is not None:
            if not check_file_content(file_client, sales_company_id):
                return (False, "SalesCompanyId does not match")
        
        return (True, "Matches all criteria")
        
    except Exception as e:
        return (False, f"Error checking file: {e}")


def move_file(file_path: str):
    """Move file from source to target location"""
    try:
        # Construct target path
        # Remove source path prefix and add target path
        if file_path.startswith(source_path):
            relative_path = file_path[len(source_path):].lstrip('/')
        else:
            relative_path = file_path.lstrip('/')
        
        target_file_path = (TARGET_PATH.rstrip('/') + '/' + relative_path).replace('//', '/')
        
        print(f"Moving: {file_path} -> {target_file_path}")
        
        # Get source and target file clients
        source_file_client = file_system_client.get_file_client(file_path)
        target_file_client = file_system_client.get_file_client(target_file_path)
        
        # Ensure target directory exists
        target_dir = '/'.join(target_file_path.split('/')[:-1])
        if target_dir:
            directory_client = file_system_client.get_directory_client(target_dir)
            if not directory_client.exists():
                directory_client.create_directory()
        
        # Copy file to target location
        # For Data Lake, we need to read and write
        download = source_file_client.download_file()
        content = download.readall()
        
        target_file_client.upload_data(
            data=content,
            overwrite=True
        )
        
        # Delete source file after successful copy
        source_file_client.delete_file()
        
        print(f"  ✓ Successfully moved: {file_path}")
        return True
        
    except Exception as e:
        print(f"  ✗ Error moving {file_path}: {e}")
        return False


def list_files_in_path(path: str) -> List[Dict]:
    """List all files in the specified path"""
    files = []
    try:
        paths = file_system_client.get_paths(path=path, recursive=True)
        
        for path_item in paths:
            if path_item.is_directory:
                continue
            
            files.append({
                'name': path_item.name,
                'size': path_item.content_length if hasattr(path_item, 'content_length') else 0,
                'last_modified': path_item.last_modified if hasattr(path_item, 'last_modified') else None,
            })
        
        print(f"Found {len(files)} files in path: {path}")
        return files
        
    except Exception as e:
        print(f"Error listing files: {e}")
        return []


def get_file_properties(file_path: str):
    """Get file properties including upload date"""
    try:
        file_client = file_system_client.get_file_client(file_path)
        return file_client.get_file_properties()
    except Exception as e:
        print(f"Error getting properties for {file_path}: {e}")
        return None


def main():
    # List all files in source path
    print(f"\nListing files in: {source_path}")
    files = list_files_in_path(source_path)
    
    if not files:
        print("No files found in source path. Exiting.")
        return
    
    # Filter files based on criteria
    print(f"\nFiltering files based on criteria...")
    files_to_move = []
    
    for file_info in files:
        file_path = file_info['name']
        file_properties = get_file_properties(file_path)
        
        if file_properties is None:
            continue
        
        should_process, reason = should_process_file(
            file_path,
            file_properties,
            FILTER_DATE_AFTER,
            FILTER_DATE_BEFORE,
            TARGET_SALES_COMPANY_ID
        )
        
        if should_process:
            files_to_move.append(file_path)
        else:
            print(f"  Skipping {file_path}: {reason}")
    
    if not files_to_move:
        print("\nNo files match the criteria. Exiting.")
        return
    
    print(f"\nFound {len(files_to_move)} files to move")
    
    # Ask for confirmation
    response = input(f"\nProceed with moving {len(files_to_move)} files? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("Operation cancelled.")
        return
    
    # Process in parallel with reduced memory footprint
    print(f"\nProcessing {len(files_to_move)} files with {MAX_WORKERS} workers...")
    
    # Process in batches to control memory usage
    BATCH_SIZE = 50
    
    successful = 0
    failed = 0
    
    for i in range(0, len(files_to_move), BATCH_SIZE):
        batch = files_to_move[i:i + BATCH_SIZE]
        print(f"\nProcessing batch {i//BATCH_SIZE + 1}/{(len(files_to_move) + BATCH_SIZE - 1)//BATCH_SIZE}")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(move_file, file_path): file_path for file_path in batch}
            for future in as_completed(futures):
                file_path = futures[future]
                try:
                    result = future.result()
                    if result:
                        successful += 1
                    else:
                        failed += 1
                except Exception as e:
                    print(f"  ✗ Exception processing {file_path}: {e}")
                    failed += 1
        
        # Force garbage collection after each batch
        gc.collect()
        print(f"Completed batch {i//BATCH_SIZE + 1} (Success: {successful}, Failed: {failed})")
    
    print(f"\n{'='*60}")
    print(f"Processing complete!")
    print(f"Successfully moved: {successful} files")
    print(f"Failed: {failed} files")
    print(f"{'='*60}")
    
    # Clean up Azure clients
    try:
        file_system_client.close()
        service_client.close()
    except:
        pass


if __name__ == "__main__":
    main()

