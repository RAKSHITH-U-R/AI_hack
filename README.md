
# GCS Downloader

This script allows you to download files from Google Cloud Storage (GCS) to a local directory efficiently. It utilizes multithreading and multiprocessing to speed up the download process.

## Features

- Downloads files from multiple GCS paths concurrently
- Utilizes multithreading for faster file downloads
- Supports multiple processes to handle multiple paths in parallel
- Configurable number of workers for multithreading
- Option to provide a service account key for authentication

## Requirements

- Python 3.6+
- `google-cloud-storage` library

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/gcs-downloader.git
cd gcs-downloader
```

2. Install the required dependencies:

```bash
pip install google-cloud-storage
```

## Usage

To use the GCS Downloader script, you can run it from the command line with the required arguments.

### Command Line Arguments

- `sources`: List of GCS paths to download files from (e.g., `gs://bucket_name/prefix`)
- `--d`: The local destination folder (default: `/mnt/disks/local_disk_1/`)
- `--k`: Path to the service account key JSON file (optional, for local use)
- `--w`: Maximum number of workers for multithreading (default: 20)

### Example

```bash
python gcs_downloader.py gs://bucket_name/prefix1 gs://bucket_name/prefix2 --d /local/destination/folder --k /path/to/service_account_key.json --w 20
```

### Code Explanation

- `GCSDownloader`: A class to handle downloading files from a GCS bucket.
- `download_blob`: Downloads a single blob (file) from a GCS bucket.
- `list_blobs`: Lists all blobs in a GCS bucket with a given prefix.
- `download_files_from_path`: Downloads all files from a specific GCS path.
- `download_files_for_path`: A helper function for multiprocessing to download files from a single path.
- `main`: The main function to handle downloading files from multiple paths using multiprocessing.


