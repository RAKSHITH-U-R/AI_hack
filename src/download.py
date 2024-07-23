import os
from google.cloud import storage
import concurrent.futures
import time

class GCSDownloader:
    def __init__(self, service_account_key_path=None, max_workers=10, destination_folder="/downloads"):
        if service_account_key_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key_path
        self.client = storage.Client()
        self.max_workers = max_workers
        self.destination_folder = destination_folder

    def download_blob(self, bucket_name, source_blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        # print(destination_file_name)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
        print(f"Downloaded {source_blob_name} to {destination_file_name}.")

    def list_blobs(self, bucket_name, prefix):
        """Lists all the blobs in the bucket that begin with the prefix."""
        bucket = self.client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        return blobs

    def download_files_from_path(self, source_path):
        """Downloads all files from a single source path."""
        # if not source_path.startswith("gs://"):
        #     raise ValueError("Source path must start with 'gs://'")

        path_parts = source_path[5:].split('/', 1)

        bucket_name = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else None
        destination_path = os.path.join(self.destination_folder, bucket_name, (prefix if prefix else ""))

        blobs = self.list_blobs(bucket_name, prefix)
        total_files = sum(1 for _ in blobs)  # Get the total number of files

        # Reset the blobs generator
        blobs = self.list_blobs(bucket_name, prefix)

        # Create the destination folder if it doesn't exist
        # if not os.path.exists(destination_path):
        os.makedirs(destination_path, exist_ok=True)

        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_blob = {
                executor.submit(
                    self.download_blob, bucket_name, blob.name, os.path.join(destination_path, os.path.basename(blob.name))
                ): blob for blob in blobs
            }

            for future in concurrent.futures.as_completed(future_to_blob):
                blob = future_to_blob[future]
                try:
                    future.result()
                except Exception as exc:
                    print(f"{blob.name} generated an exception: {exc}")

        end_time = time.time()
        print(f"Downloaded {total_files} files from {source_path} in {end_time - start_time:.2f} seconds.")

    def download_files(self, source_paths):
        """Downloads files from multiple source paths concurrently."""
        start_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self.download_files_from_path, source_path)
                for source_path in source_paths
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f"Source path generated an exception: {exc}")

        end_time = time.time()
        print(f"Completed downloading from all sources in {end_time - start_time:.2f} seconds.")

def main(source_paths, destination_folder, service_account_key_path=None):
    downloader = GCSDownloader(service_account_key_path, destination_folder=destination_folder)
    downloader.download_files(source_paths)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Download files from Google Cloud Storage.')
    parser.add_argument('--s', type=str, nargs='+', help='The source GCS paths (e.g., gs://bucket_name/prefix)')
    parser.add_argument('--d', type=str, default='/mnt/disks/local_disk_1/', help='The local destination folder')
    parser.add_argument('--k', type=str, default=None, help='Path to the service account key JSON file (optional, for local use)')

    args = parser.parse_args()

    main(args.s, args.d, args.k)
