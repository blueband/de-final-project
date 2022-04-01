import os
from concurrent.futures import as_completed
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from zipfile import ZipFile

from google.cloud import storage
from constants import *

def check_local_Source_file(file_name):
    if (len(os.listdir(INPUT_DIR)) > 0):
        if file_name.split('/')[1] in os.listdir(INPUT_DIR):
            return True
        else:
            return False


def fetch_files(blob_name, bucket):
    client = storage.client.Client(project=GCS_PROJECT)
    input_iter = client.list_blobs(bucket, delimiter="/", prefix=INPUT_PARTITION)
    archive_iter = client.list_blobs(bucket, delimiter="/", prefix=ARCHIVE_PARTITION)
    inputs = set([os.path.split(x.name)[-1] for x in input_iter])
    archive = set([os.path.split(x.name)[-1] for x in archive_iter])
    backfill = [x for x in (inputs - archive) if x.endswith(".zip")]

    current = os.path.split(blob_name)[-1]
    if current in backfill:
        backfill.remove(current)

    blob = client.bucket(bucket).blob(blob_name)
    filename = os.path.split(blob_name)[-1]
    filepath = os.path.join(INPUT_DIR, filename)
    blob.download_to_filename(filepath)
    event_dir = os.path.join(INPUT_DIR, filename.strip(".zip"))

    # extract and cleanup zip files
    with ZipFile(filepath) as zf:
        zf.extractall(event_dir)

    for file in os.listdir(event_dir):
        if file.endswith(".zip"):
            fp = os.path.join(event_dir, file)
            with ZipFile(fp) as zf:
                zf.extractall(fp.strip(".zip"))
            os.unlink(fp)
    return event_dir, backfill

def get_fetch_local_file(file_name):
    filepath = os.path.join(INPUT_DIR, file_name.split('/')[1])
    local_file = os.path.join(INPUT_DIR, file_name.split('/')[1].strip(".zip"))

    # extract and cleanup zip files
    with ZipFile(filepath) as zf:
        zf.extractall(local_file)

    for file in os.listdir(local_file):
        if file.endswith(".zip"):
            fp = os.path.join(local_file, file)
            with ZipFile(fp) as zf:
                zf.extractall(fp.strip(".zip"))
            os.unlink(fp)
    return local_file

def upload_blob(source_filepath: str, dest_filepath: str, bucket: str):
    client = storage.client.Client(project=GCS_PROJECT)
    blob = client.bucket(bucket).blob(dest_filepath)
    blob.upload_from_filename(source_filepath)


def upload_dir(source_dir: str, dest_dir: str, bucket: str):
    bucket = storage.client.Client(project=GCS_PROJECT).bucket(bucket)
    src_dest = []
    for dir_, _, files in os.walk(source_dir):
        if files:
            print(f">>> Queueing .parquet files in "
                  f"{dir_[len(source_dir):]} for upload")
        for file in files:
            src_path = os.path.join(dir_, file)
            dest_path = os.path.relpath(dest_dir + src_path[len(source_dir):])
            src_dest.append((src_path, dest_path))
    func = lambda x: bucket.blob(x[1]).upload_from_filename(x[0])
    concurrent_exec(func, src_dest, max_workers=10)
    return



def concurrent_exec(func, iterable, context=(), max_workers=None, method="thread"):
    """
    use multiprocessing to call a function on members of an iterable
    :param func: the function to call
    :param iterable: items you want to call func on
    :param context: params that are passed to all instances
    :param max_workers: maximum number of child processes to use;
                        if 0, we don't use concurrency
    :param method: process | thread (default)
    :return:
    """
    retval = []
    methods = {
        "process": ProcessPoolExecutor,
        "thread": ThreadPoolExecutor
    }
    if max_workers == 0:
        for item in iterable:
            retval.append(func(item, *context))
    else:
        Executor = methods.get(method or "process")
        print(f"Using {Executor.__name__} to run upload jobs...")
        with Executor(max_workers=max_workers) as executor:
            future_to_item = {executor.submit(func, item, *context): item for item in iterable}
            for future in as_completed(future_to_item):
                try:
                    retval.append(future.result())
                except Exception as exc:
                    pass
    return retval




def get_current_month_file(file_month):
    source_file_name = {
            "bucket": GCS_BUCKET,
            "name": f"Input/{file_month}.zip",
            "metageneration": "",
            "timeCreated": "",
            "updated": ""
        }

    return source_file_name