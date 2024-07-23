import os
from google.cloud import storage
from multiprocessing import Pool, cpu_count
import time

class GCSDownloader:
    def __init__(self, service_account_key_path=None, max_workers=20, destination_folder="/downloads"):
        if service_account_key_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key_path
        self.max_workers = max_workers or cpu_count()
        self.destination_folder = destination_folder

    def download_blob(self, bucket_name, source_blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        client = storage.Client()  # Initialize client inside the worker process
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
        print(f"Downloaded {source_blob_name} to {destination_file_name}.")

    def list_blobs(self, bucket_name, prefix):
        """Lists all the blobs in the bucket that begin with the prefix."""
        client = storage.Client()  # Initialize client inside the function
        bucket = client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        return blobs

    def prepare_download_tasks(self, source_path):
        """Prepare a list of download tasks from a source path."""
        path_parts = source_path[5:].split('/', 1)
        bucket_name = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else None
        destination_path = os.path.join(self.destination_folder, bucket_name, (prefix if prefix else ""))

        blobs = self.list_blobs(bucket_name, prefix)
        os.makedirs(destination_path, exist_ok=True)

        tasks = []
        for blob in blobs:
            destination_file_name = os.path.join(destination_path, os.path.basename(blob.name))
            tasks.append((bucket_name, blob.name, destination_file_name))
        return tasks

    def download_files_from_path(self, source_path):
        """Downloads all files from a single source path."""
        tasks = self.prepare_download_tasks(source_path)
        print(f"Started downloading files from {source_path}")
        start_time = time.time()

        # Create a Pool of workers
        with Pool(processes=self.max_workers) as pool:
            pool.starmap(self.download_blob, tasks)

        end_time = time.time()
        print(f"Downloaded {len(tasks)} files from {source_path} in {end_time - start_time:.2f} seconds.")

    def download_files(self, source_paths):
        """Downloads files from multiple source paths concurrently."""
        start_time = time.time()

        # Create a Pool of workers
        # with Pool(processes=self.max_workers) as pool:
        #     pool.map(self.download_files_from_path, source_paths)
        for source in source_paths:
            self.download_files_from_path(source)

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
