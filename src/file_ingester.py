# TODO 
# connect to google drive
# use supply parameter month/or current moth to retrive month data
# push data to cloud bucket

from datetime import datetime
import pickle
import os
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import io, shutil
from googleapiclient.http import MediaIoBaseDownload

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

    month_params = '30'+ '_' + str(current_month) + '_' + str(current_year)
    return month_params


def get_gdrive_service():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    # return Google Drive API service
    return build('drive', 'v3', credentials=creds)


def search(service, month, year):
    month_params = get_current_month(month, year)
    # service = get_gdrive_service()
    all_blobs = service.files().list().execute()
    files = all_blobs['files']
    for file in files:
        for k, v in file.items():
            if month_params not in v: return False
            else:
                file_name = file['name']
                fileID = file['id']
    # print(f'https://drive.google.com/file/d/{fileID}/')
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



def main(month=None, year=None):
    service = get_gdrive_service()
    # filetype = "text/plain"
    # search for files that has type of text/plain
    search_term = search(service, month, year)
    if search_term:
        fileID, file_name = search_term[0], search_term[1]
        downloader(service, fileID, file_name)
    else:
        print('File not found')


if __name__ == '__main__':
    main()