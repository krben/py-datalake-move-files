import config
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc
import pandas as pd
import os

# Config
tenant_id = config.AZURE_TENANT_ID
client_id = config.AZURE_CLIENT_ID
client_secret = config.AZURE_CLIENT_SECRET
account_name = config.AZURE_ACCOUNT_NAME
container_name = config.AZURE_CONTAINER_NAME
source_prefix = config.AZURE_REMOTE_PATH     # e.g., "raw/"
target_prefix = "/files/sbt/quotes/"     # e.g., "archive/"

# CSV file path - update this to your actual CSV file path
CSV_FILE_PATH = "quotes/archived_quotes.csv"

print(f"Using CSV file: {CSV_FILE_PATH}")
print(f"Source prefix: {source_prefix}")
print(f"Target prefix: {target_prefix}")

# Auth & Client Setup
credential = ClientSecretCredential(tenant_id, client_id, client_secret)
blob_service_client = BlobServiceClient(
    account_url=f"https://{account_name}.blob.core.windows.net",
    credential=credential
)
container_client = blob_service_client.get_container_client(container_name)

MAX_WORKERS = 16  # Tune this based on your machine/network

def copy_and_delete_blob(blob_name):
    """Copy blob to target location and delete original"""
    try:
        source_blob_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}"
        
        # Debug the replace operation
        print(f"Source prefix: '{source_prefix}'")
        print(f"Target prefix: '{target_prefix}'")
        print(f"Original blob name: '{blob_name}'")
        
        target_blob_name = blob_name.replace(source_prefix, target_prefix, 1)
        print(f"After replace: '{target_blob_name}'")
        
        # If replace didn't work, manually construct the target path
        if target_blob_name == blob_name:
            # Extract the filename part and construct new path
            filename = blob_name.split('/')[-1]
            target_blob_name = target_prefix.rstrip('/') + '/' + filename
            print(f"Manual construction: '{target_blob_name}'")
        
        print(f"Copying: {blob_name} -> {target_blob_name}")
        
        target_blob_client = container_client.get_blob_client(target_blob_name)
        # Start copy
        copy_props = target_blob_client.start_copy_from_url(source_blob_url)
        print(f"Copy started for {blob_name}: {copy_props['copy_id']}")
        
        # Optionally, wait for copy to complete before deleting
        # while target_blob_client.get_blob_properties().copy.status == 'pending':
        #     time.sleep(0.1)
        # Delete original
        # container_client.delete_blob(blob_name)
        print(f"Successfully initiated copy: {blob_name}")
    except Exception as e:
        print(f"Error copying {blob_name}: {e}")


def get_filenames_from_csv(csv_file_path):
    """Read filenames from CSV file"""
    try:
        if not os.path.exists(csv_file_path):
            print(f"CSV file not found: {csv_file_path}")
            return []
        
        # Read the CSV file
        df = pd.read_csv(csv_file_path, delimiter=';')
        
        # Check if 'filename' column exists
        if 'filename' not in df.columns:
            print("Error: 'filename' column not found in CSV file")
            print(f"Available columns: {list(df.columns)}")
            return []
        
        # Get list of filenames, filtering out any NaN values
        filenames = df['filename'].dropna().tolist()
        
        print(f"Loaded {len(filenames)} filenames from CSV: {csv_file_path}")
        if filenames:
            print(f"Sample filenames: {filenames[:3]}")
        
        return filenames
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []

def filter_blobs_by_csv_filenames(csv_filenames):
    """Filter blobs based on filenames from CSV"""
    print(f"Filtering blobs based on {len(csv_filenames)} filenames from CSV")
    
    # Convert CSV filenames to full blob paths if they don't include the source prefix
    csv_blob_paths = []
    for filename in csv_filenames:
        if filename.startswith(source_prefix):
            csv_blob_paths.append(filename)
        else:
            # Add source prefix if not present
            csv_blob_paths.append(f"{source_prefix.rstrip('/')}/{filename}")
    
    print(f"Looking for {len(csv_blob_paths)} specific blob paths")
    
    # Check each CSV filename individually instead of listing all blobs
    matching_blobs = []
    not_found_blobs = []
    
    for blob_path in csv_blob_paths:
        try:
            # Check if this specific blob exists
            blob_client = container_client.get_blob_client(blob_path)
            if blob_client.exists():
                matching_blobs.append(blob_path)
                print(f"Found: {blob_path}")
            else:
                not_found_blobs.append(blob_path)
                print(f"Not found: {blob_path}")
        except Exception as e:
            print(f"Error checking {blob_path}: {e}")
            not_found_blobs.append(blob_path)
    
    print(f"\nSummary:")
    print(f"Found {len(matching_blobs)} matching blobs from CSV")
    print(f"Not found: {len(not_found_blobs)} blobs")
    
    # Show some examples
    if matching_blobs:
        print("\nTop 10 matching blobs:")
        for i, blob_name in enumerate(matching_blobs[:10]):
            print(f"  {i+1}. {blob_name}")
        
        if len(matching_blobs) > 10:
            print(f"  ... and {len(matching_blobs) - 10} more blobs")
    
    if not_found_blobs:
        print(f"\nFirst 5 not found blobs:")
        for i, blob_name in enumerate(not_found_blobs[:5]):
            print(f"  {i+1}. {blob_name}")
        
        if len(not_found_blobs) > 5:
            print(f"  ... and {len(not_found_blobs) - 5} more not found")
    
    return matching_blobs

def main():
    # Get filenames from CSV
    csv_filenames = get_filenames_from_csv(CSV_FILE_PATH)
    if not csv_filenames:
        print("No valid filenames found in CSV. Exiting.")
        return
    
    # Filter blobs based on CSV filenames
    blob_names_to_process = filter_blobs_by_csv_filenames(csv_filenames)
    
    if len(blob_names_to_process) == 0:
        print("No matching blobs found for CSV filenames. Check your source prefix and CSV content.")
        return
    
    # Process in parallel with reduced memory footprint
    print(f"Processing {len(blob_names_to_process)} blobs with {MAX_WORKERS} workers...")
    
    # Process in batches to control memory usage
    BATCH_SIZE = 50  # Process 50 blobs at a time
    
    for i in range(0, len(blob_names_to_process), BATCH_SIZE):
        batch = blob_names_to_process[i:i + BATCH_SIZE]
        print(f"Processing batch {i//BATCH_SIZE + 1}/{(len(blob_names_to_process) + BATCH_SIZE - 1)//BATCH_SIZE}")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(copy_and_delete_blob, blob_name) for blob_name in batch]
            for future in as_completed(futures):
                pass  # Optionally handle results or exceptions here
        
        # Force garbage collection after each batch
        gc.collect()
        print(f"Completed batch {i//BATCH_SIZE + 1}")
    
    print("Processing complete!")
    
    # Clean up Azure clients
    try:
        container_client.close()
        blob_service_client.close()
    except:
        pass  # Clients may not have close methods in all versions

if __name__ == "__main__":
    main() 