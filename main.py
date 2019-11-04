import boto3
from botocore.exceptions import NoCredentialsError
import git
import pickle
import tempfile
import datetime
import os
import shutil
import io
import sys
import json
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from dotenv import load_dotenv
load_dotenv()


def create_zip_file_from_repo(clone_url, filename, branch="master"):
    """
    Method that creates a zip file from a repo URL and creates it with the name provided by the user
    @params [clone_url, filename]
    - clone_url: URL from the remote repo.
    - filename: Name provided for the zip file to be created.
    """
    print("Cloning repo " + clone_url +" into " + filename + ".zip")
    repo = git.Repo.clone_from(clone_url, to_path=os.path.join(tempfile.mkdtemp()), multi_options=['-b ' + branch])

    with open(filename + '.zip', "wb") as zipfile:
        repo.archive(zipfile, format='zip')
        return zipfile.name


def get_drive_service():
    """
    Function to connect with the service driver that connects with Google Drive API
    @params []
    """

    print("Connecting to Google Drive...")

    # If modifying these scopes, delete the file token.pickle.
    SCOPES = ['https://www.googleapis.com/auth/drive']

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

    # Service driver to interact with the Drive API
    return build('drive', 'v3', credentials=creds)


def clear_folder(folder_id, service):
    """
    Delete all the files inside the selected folder.
    @params [folder_id]
    - folder_id: Google Drive id of the folder to be cleared
    - service: Driver manager of the Google Drive API
    """

    print("Cleaning up the folder...")

    # Search files in the folder described by folder_id
    files = service.files().list(
        q="'" + folder_id + "' in parents"
    ).execute().get('files', [])

    # Deleting previous files in folder
    for f in files:
        service.files().delete(fileId=f['id']).execute()


def upload_file_to_drive(folder_id, path, name, service):
    """
    Method that uploads a file to a specific folder in Google Drive
    @params [folder_id, path, filename]
    - folder_id: Google Drive id of the folder where the file will be stored in
    - path: Actual location of file to upload
    - name: Name of the local file
    - service: Driver manager of the Google Drive API
    """

    print("Uploading file " + path + "...")

    # Filename construction
    prefix = "code_v"
    now = datetime.datetime.now()
    date = now.strftime("%Y%m%d%H%M%S")
    filename = prefix + date + name + ".zip"

    file_metadata = {
        'name': filename,
        'mimeType': 'application/zip',
        'parents': [folder_id]
    }

    media = MediaFileUpload(path,
                            mimetype='application/zip',
                            resumable=True)
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(filename +' uploaded with succes. File ID: ' + file.get('id'))


def download_file_from_drive(file_id, location, filename, service):
    """
    It downloads a file from Drive to local.
    @params [file_id, location, filename, service]
    - file_id: Google Drive id of the file
    - location: Place to save the file
    - filename: Name of the new file created locally
    - service: Driver manager of the Google Drive API
    """
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(location + filename, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        try:
            status, done = downloader.next_chunk()
        except:
            fh.close()
            os.remove(location + filename)
            sys.exit(1)
        print(f'\rDownload {int(status.progress() * 100)}%.', end='')
        sys.stdout.flush()
    print('')


def download_gdoc_from_drive(file_id, mime_type, location, filename, service):
    """
    It downloads a file from Drive to local.
    @params [file_id, location, filename, service]
    - file_id: Google Drive id of the file
    - location: Place to save the file
    - filename: Name of the new file created locally
    - service: Driver manager of the Google Drive API
    """
    new_mime_type = ''
    if mime_type == 'application/vnd.google-apps.document':
        new_mime_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    elif mime_type == 'application/vnd.google-apps.spreadsheet':
        new_mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    elif mime_type == 'application/vnd.google-apps.drawing':
        new_mime_type = 'image/jpeg'
    elif mime_type == 'application/vnd.google-apps.presentation':
        new_mime_type = 'application/vnd.openxmlformats-officedocument.presentationml.presentation'

    request = service.files().export_media(fileId=file_id, mimeType=new_mime_type)

    fh = io.FileIO(location + filename, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        try:
            status, done = downloader.next_chunk()
        except:
            fh.close()
            os.remove(location + filename)
            sys.exit(1)
        print(f'\rDownload {int(status.progress() * 100)}%.', end='')
        sys.stdout.flush()
    print('')



def download_folder_from_drive(folder_id, location, folder_name, service):
    """
    It downloads a directory recursively from Drive to local.
    @params [folder_id, location, folder_name, service]
    - folder_id: Google Drive id of the folder where the file will be downloaded
    - location: Place to save the folder
    - folder_name: Name of the new folder created locally
    - service: Driver manager of the Google Drive API
    """

    if not os.path.exists(location + folder_name):
        os.makedirs(location + folder_name)
    location += folder_name + '/'

    result = []
    files = service.files().list(
        pageSize='1000',
        q=f"'{folder_id}' in parents").execute()

    result.extend(files['files'])
    result = sorted(result, key=lambda k: k['name'])

    total = len(result)
    current = 1
    for item in result:
        file_id = item['id']
        filename = item['name']
        mime_type = item['mimeType']
        print(f'{file_id} {filename} {mime_type} ({current}/{total})')
        if mime_type == 'application/vnd.google-apps.folder':
            download_folder_from_drive(file_id, location, filename, service)
        elif not os.path.isfile(location + filename) and (
                mime_type == 'application/vnd.google-apps.document' or
                mime_type == 'application/vnd.google-apps.drawing' or
                mime_type == 'application/vnd.google-apps.presentation' or
                mime_type == 'application/vnd.google-apps.spreadsheet'):
            download_gdoc_from_drive(file_id, mime_type, location, filename, service)
        elif not os.path.isfile(location + filename):
            download_file_from_drive(file_id, location, filename, service)
        current += 1


def zip_folder(folder_name):
    """
    Method to zip a folder
    @params [folder_name]
    - folder_name: Local folder to be compressed
    """

    shutil.make_archive(folder_name, 'zip', folder_name)


def upload_file_to_s3(local_file, bucket, s3_file):
    """
    Method used to store a file in S3
    @params [local_file, bucket, s3_file]
    - local_file: Name of the local file to be uploaded
    - bucket: Name of the bucket
    - s3_file: Name that the file will receive once uploaded
    """

    s3 = boto3.client('s3', aws_access_key_id=os.getenv('S3_ACCESS_KEY'),
                      aws_secret_access_key=os.getenv('S3_SECRET_KEY'))

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


def clear_local_folder(project_name):
    """
    After the process is completed, all the local files are removed
    @params [local_file, bucket, s3_file]
    - project_name: Name of the project (Also the name of the doc directory
    """
    for file in os.listdir("."):
        if file.endswith('.zip'):
            os.remove(file)
        elif file.endswith(project_name):
            shutil.rmtree(file)


if __name__ == "__main__":
    google_drive_doc_folder_id = os.getenv('GOOGLE_DRIVE_DOC_FOLDER_ID')
    google_drive_code_folder_id = os.getenv('GOOGLE_DRIVE_CODE_FOLDER_ID')
    project_name = os.getenv('PROJECT_NAME')
    s3_access_key = os.getenv('S3_ACCESS_KEY')
    s3_secret_key = os.getenv('S3_SECRET_KEY')
    s3_bucket_name = os.getenv('S3_BUCKET')

    with open('repos.json', 'r') as f:
        repos = json.load(f)

    # Connecting to the GDrive API
    service = get_drive_service()

    # Clearing the code folder
    clear_folder(google_drive_code_folder_id, service)

    # For each repo it is created a local zipfile that is uploaded to the Drive code folder.
    for repo in repos:
        path = create_zip_file_from_repo(repo['url'], repo['name'], repo['branch'])
        upload_file_to_drive(google_drive_code_folder_id, path, repo['name'], service)

    # Lastly, it is downloaded and compressed the documentation folder just to be uploaded to the S3 bucket.
    download_folder_from_drive(google_drive_doc_folder_id, './', project_name, service)
    zip_folder(project_name)

    prefix = "doc_v"
    now = datetime.datetime.now()
    date = now.strftime("%Y%m%d%H%M%S")
    s3_filename = prefix + date + "_" + project_name + ".zip"

    upload_file_to_s3(project_name + ".zip", s3_bucket_name, s3_filename)
    clear_local_folder(project_name)


