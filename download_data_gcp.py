import sys
import argparse
import signal
from google.cloud import storage
from pathlib import Path


def exponential_retry(func, args=[], kwargs={}, starting_timeout=1, max_retries=5):
    """
    Notes:
    This is super hacky, creating and receiving a unix SIGALRM as the timer
    mechanism. This also doesn't make any attempt cleanly terminate the
    blocking function call
    """
    def timeoutHandler(signum, frame):
        raise TimeoutError
    signal.signal(signal.SIGALRM, timeoutHandler)
    retry_count = 0
    while True:
        try:
            signal.alarm(starting_timeout)
            func(*args, **kwargs)
            signal.alarm(0)
            break
        except TimeoutError:
            starting_timeout *= 2
            retry_count += 1
            if retry_count > max_retries:
                break
            continue


def main() -> int:
    """
    Notes:
    * This intentionally only supports GCP, hence the script name. YAGNI for
    other blob storage services
    * Most likely, this data wouldn't be stored in a public bucket. This script
    will inevitably need to support credentials
    * I'm intentionally not catching any exceptions on the GCP Client, as the
    bare exception is probably more useful than anything I can throw
    * This uses GCP's default retry policy, defined here
    https://googleapis.dev/python/google-api-core/latest/retry.html#google.api_core.retry.Retry
    with a 1 second initial backoff, a backoff multiplier of 2, and a 2 minute timeout

    """
    parser = argparse.ArgumentParser(prog='download_data_gcp.py',
                                     description='Downloads all files from a GCP bucket')
    parser.add_argument("-d",
                        "--dest_dir",
                        help="Destination to store files")
    parser.add_argument("bucket_name",
                        help="Bare name of the Google Cloud Storage bucket")

    args = parser.parse_args()
    bucket_name = args.bucket_name
    dest_dir = args.dest_dir
    if dest_dir is None:
        dest_dir = "."

    storage_client = storage.Client.create_anonymous_client()
    blobs = storage_client.list_blobs(bucket_name)
    for blob in blobs:
        filepath = Path(dest_dir, blob.name)
        exponential_retry(blob.download_to_filename, [filepath], {'retry': None})

    return 0


if __name__ == '__main__':
    sys.exit(main())
