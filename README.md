# Azure Data Lake File Mover

A Python application that moves files in Azure Blob Storage from a source location to a target location based on filenames listed in a CSV file. This tool is designed to efficiently archive files using parallel processing.

## Overview

This project automates the process of moving files (specifically quote files) in Azure Blob Storage. It reads a list of filenames from a CSV file and copies the corresponding blobs from a source prefix path to a target prefix path. The application uses multi-threaded processing to handle large batches of files efficiently.

## Features

- **CSV-based file selection**: Reads filenames from a CSV file to determine which files to move
- **Parallel processing**: Uses ThreadPoolExecutor with configurable worker threads for efficient batch processing
- **Memory management**: Processes files in batches with garbage collection to manage memory usage
- **Azure authentication**: Uses service principal authentication (ClientSecretCredential) for secure access
- **Error handling**: Provides detailed logging and error reporting for each file operation
- **Path transformation**: Automatically transforms blob paths from source to target locations

## Requirements

- Python 3.12+
- Azure Storage Account with Blob Storage
- Service Principal credentials with appropriate permissions

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd py-datalake-move-files
```

2. Create a virtual environment:
```bash
python -m venv venv
```

3. Activate the virtual environment:
- On Windows:
```bash
venv\Scripts\activate
```
- On Linux/Mac:
```bash
source venv/bin/activate
```

4. Install dependencies:
```bash
pip install -r requirements/requirements.txt
```

## Configuration

1. Create a `.env` file in the project root with the following variables:

```env
# Azure Storage Configuration
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret
AZURE_ACCOUNT_NAME=your-storage-account-name
AZURE_CONTAINER_NAME=your-container-name
AZURE_REMOTE_PATH=raw/  # Source path prefix
```

2. Update the CSV file path and target prefix in `app/app.py` if needed:
   - `CSV_FILE_PATH`: Path to your CSV file containing filenames
   - `target_prefix`: Target path where files will be copied (default: `/files/sbt/quotes/`)

## CSV File Format

The CSV file should contain a `filename` column with the names of files to move. The file uses semicolon (`;`) as the delimiter.

Example CSV structure:
```csv
QuoteId;unixtimestamp;filename
6444d38f-2f5e-4ceb-b0f2-0cc5f8c9444a;1758085060;1758085060_6444d38f-2f5e-4ceb-b0f2-0cc5f8c9444a.json
ae0ffb74-a0f2-4e7d-824f-d4d674aed3a5;1757094111;1757094111_ae0ffb74-a0f2-4e7d-824f-d4d674aed3a5.json
```

The application will:
- Read filenames from the `filename` column
- Prepend the source prefix if not already present
- Check if each blob exists in Azure Storage
- Copy matching blobs to the target location

## Usage

1. Ensure your `.env` file is configured with valid Azure credentials
2. Place your CSV file in the `app/quotes/` directory (or update `CSV_FILE_PATH` in `app.py`)
3. Run the application:
```bash
python app/app.py
```

## How It Works

1. **CSV Reading**: Reads filenames from the specified CSV file
2. **Blob Filtering**: Checks each filename from the CSV against Azure Blob Storage to find matching blobs
3. **Path Transformation**: Transforms blob paths from source prefix to target prefix
4. **Parallel Copying**: Uses ThreadPoolExecutor to copy blobs in parallel (default: 4 workers)
5. **Batch Processing**: Processes files in batches of 50 to manage memory efficiently
6. **Logging**: Provides detailed output about found/missing files and copy operations

## Configuration Options

In `app/app.py`, you can adjust:

- `MAX_WORKERS`: Number of parallel threads (default: 4)
- `BATCH_SIZE`: Number of files processed per batch (default: 50)
- `CSV_FILE_PATH`: Path to the CSV file
- `target_prefix`: Target location for copied files

## Notes

- The application currently **initiates** the copy operation but does **not delete** the original files (delete is commented out in the code)
- Copy operations are asynchronous - the application starts the copy but doesn't wait for completion
- Files are checked individually rather than listing all blobs, which is more efficient for large containers

## Error Handling

The application handles:
- Missing CSV files
- Missing `filename` column in CSV
- Non-existent blobs in Azure Storage
- Individual copy operation failures (continues processing other files)

## Dependencies

Key dependencies:
- `azure-storage-blob`: Azure Blob Storage SDK
- `azure-identity`: Azure authentication
- `pandas`: CSV file processing
- `python-dotenv`: Environment variable management

See `requirements/requirements.txt` for the complete list.

## License

[Specify your license here]

