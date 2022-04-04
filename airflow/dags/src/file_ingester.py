# TODO 
# connect to google drive
# use supply parameter month/or current moth to retrive month data
# push data to cloud bucket

from datetime import datetime
import pickle
import os
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import io, shutil
from googleapiclient.http import MediaIoBaseDownload

sub_path = '/opt/airflow/dags/'
# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

def get_current_month(current_month=None, current_year=None):
    if (current_month is None) and (current_year is None):
        current_month = datetime.now().month
        current_year = datetime.now().year
    elif current_year is None:
        current_year = datetime.now().year
    elif current_month is None:
        current_month = datetime.now().month

    month_params = '30'+ '_' + str(current_month) + '_' + str(current_year) + '.zip'
    return month_params

def get_file_path(file_name):
    return os.path.join( os.getcwd(), file_name)
    

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)





def get_gdrive_service():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(sub_path +'token.pickle'):
        with open(sub_path + 'token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(sub_path + 'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    # return Google Drive API service
    return build('drive', 'v3', credentials=creds, cache_discovery=False)


def search(service, month, year):
    temp_container = dict()
    month_params = get_current_month(month, year)
    # service = get_gdrive_service()
    all_blobs = service.files().list().execute()
    files = all_blobs['files']
    for file in files:
        temp_container[file['id']] = file['name']
    temp_file = list(temp_container.values())
    if month_params not in temp_file: return False
    else:
        for k,v in temp_container.items():
            if month_params == v:
                file_name = v
                fileID = k
    return (fileID, file_name)



def downloader(service, fileid, file_name):
    request = service.files().get_media(fileId=fileid)
    fh = io.BytesIO()
    # fh = io.FileIO
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        
    # https://stackoverflow.com/questions/60111361/how-to-download-a-file-from-google-drive-using-python-and-the-drive-api-v3
    fh.seek(0)
    with open(file_name, 'wb') as f:
        shutil.copyfileobj(fh, f, length=131072)



    print(f'File with name {file_name} download completely')
    return get_file_path(file_name)



def download_main(month=None, year=None, **kwargs):
    service = get_gdrive_service()
    # search for files that has type of text/plain
    search_term = search(service, month, year)
    if search_term:
        fileID, file_name = search_term[0], search_term[1]
        file_path = downloader(service, fileID, file_name)
        # task_instance = kwargs['task_instance']
        # task_instance.xcom_push(key="file_path", value=file_path)
        return file_path
    else:
        print('File not found ! Check back in few days')



def get_file_name(file_path):
    fileName = file_path.split('/')
    return fileName[-1]