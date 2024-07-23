import os
from google.cloud import storage
import concurrent.futures
import multiprocessing
import time

class GCSDownloader:
    def __init__(self, service_account_key_path=None, max_workers=20, destination_folder="/downloads"):
        if service_account_key_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key_path
        self.client = storage.Client()
        self.max_workers = max_workers
        self.destination_folder = destination_folder

    def download_blob(self, bucket_name, source_blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)

    def list_blobs(self, bucket_name, prefix):
        """Lists all the blobs in the bucket that begin with the prefix."""
        bucket = self.client.bucket(bucket_name)
        return list(bucket.list_blobs(prefix=prefix))

    def download_files_from_path(self, source_path):
        """Downloads all files from a single source path."""
        path_parts = source_path[5:].split('/', 1)
        bucket_name = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else None
        destination_path = os.path.join(self.destination_folder, bucket_name, (prefix if prefix else ""))

        blobs = self.list_blobs(bucket_name, prefix)
        total_files = len(blobs)  # Get the total number of files

        # Create the destination folder if it doesn't exist
        os.makedirs(destination_path, exist_ok=True)
        print(f"Started downloading {total_files} files from {source_path}")
        start_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(
                    self.download_blob, bucket_name, blob.name, os.path.join(destination_path, os.path.basename(blob.name))
                ) for blob in blobs
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f"Download generated an exception: {exc}")

        end_time = time.time()
        print(f"Downloaded {total_files} files from {source_path} in {end_time - start_time:.2f} seconds.")

def download_files_for_path(args):
    """Function to be used with multiprocessing to download files from a single source path."""
    source_path, service_account_key_path, destination_folder, max_workers = args
    downloader = GCSDownloader(service_account_key_path=service_account_key_path,
                               max_workers=max_workers,
                               destination_folder=destination_folder)
    downloader.download_files_from_path(source_path)

def main(source_paths, destination_folder, service_account_key_path=None, max_workers=20):
    start_time = time.time()

    pool_args = [(source_path, service_account_key_path, destination_folder, max_workers) for source_path in source_paths]

    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        pool.map(download_files_for_path, pool_args)

    end_time = time.time()
    print(f"Completed downloading from all sources in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Download files from Google Cloud Storage.')
    parser.add_argument('sources', type=str, nargs='+', help='The source GCS paths (e.g., gs://bucket_name/prefix)')
    parser.add_argument('--d', type=str, default='/mnt/disks/local_disk_1/', help='The local destination folder')
    parser.add_argument('--k', type=str, default=None, help='Path to the service account key JSON file (optional, for local use)')
    parser.add_argument('--w', type=int, default=20, help='Maximum number of workers for multithreading')

    args = parser.parse_args()

    main(args.sources, args.d, args.k, args.w)
