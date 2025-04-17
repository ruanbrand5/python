import functions_framework
import re
import io

from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.discovery import Resource
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from pypdf import PdfReader, PdfWriter

# Define the scope
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly'] # 'https://www.googleapis.com/auth/spreadsheets',

SERVICE_ACCOUNT_FILE = "path/to/file.json"

@functions_framework.http
def http_request_handler(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.

    {
        "pdfs": [
            {
            "url": "https://drive.google.com/file/d/[id here]/view?usp=drive_link",
            "password": "1234",
            "protect": true
            }
        ],
        "naming": "Protected_"
    }
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    access_token = request.headers.get('Authorization').split(' ')[1]

    # Get the masterUrl from the POST object
    pdfs_to_encrypt = []
    if request_json and 'pdfs' in request_json:
        pdfs_to_encrypt = request_json['pdfs']
    elif request_args and 'pdfs' in request_args:
        pdfs_to_encrypt = request_args['pdfs']

    naming_prefix = ""
    if request_json and 'naming' in request_json:
        naming_prefix = request_json['naming']
    elif request_args and 'naming' in request_args:
        naming_prefix = request_args['naming']

    # Check if the request was a POST request
    if request.method != "POST":
        # not adding "Aborting" to the log because that triggers a different alert
        # print("Invalid method")
        return "Invalid method", 500
        # flask.abort(500)

    # Check if all required parameters are present
    if not pdfs_to_encrypt:
        # print("Missing required parameters")
        return "Missing required parameters", 400

    encrypted_pdfs = encrypt_pdfs(access_token, pdfs_to_encrypt, naming_prefix)

    return encrypted_pdfs, 200

def encrypt_pdfs(access_token, pdfs: list[dict], naming_prefix: str) -> list[str]:
    """
    Returns the list of encrypted pdf URLs

    Args:
        pdfs (list[dict]): The list of PDF dict to be encrypted.
            'url': The URL of the PDF
            'password': The password of the encrypted PDF
            'protect': Bool to indicate if file needs to be protected or not
        naming_prefix (str): String that needs to be added to beginning of each PDF name

    Returns:
        dict: The File Resource of the found or created folder

    Raises:
        Exception: If an error occured.
    """
    try:
        # creds = get_credentials(SERVICE_ACCOUNT_FILE, SCOPES)
        creds = Credentials(token=access_token)
        drive_service = create_service('drive', "v3", credentials=creds)

        encrypted_pdfs = []
        for file in pdfs:
            print(file)
            if file["protect"]:
                file_id = get_id_from_url(file["url"])
                password = file["password"]

                drive_file = get_file(drive_service, file_id)
                if drive_file:
                    pdf_stream = stream_pdf_from_drive(drive_service, file_id)
                    encrypted_pdf_stream = psw_protect_pdf(pdf_stream, password)

                    print("Current File:")
                    print(drive_file)
                    file_name = drive_file.get("name", "")
                    parents = drive_file.get("parents", None)

                    new_file_name = f"{naming_prefix}{file_name}"
                    new_file = upload_pdf_to_drive(drive_service, encrypted_pdf_stream, new_file_name, parents)
                    encrypted_pdfs.append(new_file)
                    print(f"Encrypted PDF uploaded successfully with ID: {new_file}")
            else:
                encrypted_pdfs.append(file["url"])
        return encrypted_pdfs

    except Exception as e:
        print(f'An error occurred: {e}')
        return None


def stream_pdf_from_drive(drive_service: Resource, file_id: str) -> io.BytesIO:
    """
    Download a file from Google Drive as a stream.

    Args:
        drive_service (Resource): The authenticated drive service instance
        file_id (str): The id of the Google Gile to stream

    Returns:
        file_stream (BytesIO): The Google File's stream

    Raises:
        Exception: If an error occured.
    """
    try:
        request = drive_service.files().get_media(fileId=file_id)
        file_stream = io.BytesIO()
        downloader = MediaIoBaseDownload(file_stream, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        file_stream.seek(0)  # Reset stream position
        return file_stream
    except Exception as e:
        print(f'An error occurred: {e}')
        return None

def psw_protect_pdf(pdf_stream: io.BytesIO, password: str) -> io.BytesIO:
    """
    Password Protect a PDF in memory.

    Args:
        pdf_stream (io.BytesIO): The pdf file stream
        password (str): The password to encrypt the PDF with

    Returns:
        encrypted_stream (io.BytesIO): The encrypted pdf file stream

    Raises:
        Exception: If an error occured.
    """
    try:
        reader = PdfReader(pdf_stream)
        writer = PdfWriter()
        
        for page in reader.pages:
            writer.add_page(page)
        
        writer.encrypt(password)
        
        encrypted_stream = io.BytesIO()
        writer.write(encrypted_stream)
        encrypted_stream.seek(0)
        return encrypted_stream

    except Exception as e:
        print(f'An error occurred: {e}')
        return None

def upload_pdf_to_drive(drive_service: Resource, encrypted_stream: io.BytesIO, filename: str, parent_folder_id: str = None) -> dict:
    """
    Upload an encrypted PDF to Google Drive.

    Args:
        drive_service (Resource): The authenticated drive service instance
        encrypted_stream (io.BytesIO): The encrypted pdf file stream
        filename (str): The name of the file to save the stream as
        parent_folder_id (str): The id of the drive folder to save the file in

    Returns:
        file (dict): The newly created file in Google Drive

    Raises:
        Exception: If an error occured.
    """
    try:
        file_metadata = {
            "name": f"{filename}"
        }

        if parent_folder_id:
            file_metadata["parents"] = parent_folder_id
        
        media = MediaIoBaseUpload(encrypted_stream, mimetype="application/pdf", resumable=True)
        file = drive_service.files().create(
            supportsAllDrives=True,
            body=file_metadata, 
            media_body=media, 
            fields="kind,id,name,mimeType,parents,webViewLink"
        ).execute()
        
        return file #.get("id")
    except Exception as e:
        print(f'An error occurred: {e}')
        return None

def get_folder(drive_service: Resource, parent_folder_id: str, folder_name: str) -> dict:
    """
    Returns the folder with the specified name in the specified parent folder. 
    If the folder can't be found, it will create the folder and return it.

    Args:
        drive_service (Resource): The authenticated drive service instance
        parent_folder_id (str): The id of the parent folder
        folder_name (str): The name of the folder to search for

    Returns:
        dict: The File Resource of the found or created folder

    Raises:
        Exception: If an error occured.
    """
    try:
        query = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and trashed=false"
        response = drive_service.files().list(q=query, supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        folders = response.get('files', [])
        if folders:
            return folders[0]
        else:
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_folder_id]
            }
            folder = drive_service.files().create(body=folder_metadata, supportsAllDrives=True).execute()
            return folder
    except Exception as e:
        print(f'An error occurred: {e}')
        return None

def get_file(drive_service: Resource, file_id: str) -> dict:
    """
    Returns the file with the given id.

    Args:
        drive_service (Resource): The authenticated drive service instance
        file_id (str): The id of the file

    Returns:
        dict: The File Resource of the file

    Raises:
        Exception: If an error occured.
    """
    try:
        response = drive_service.files().get(
            fileId=file_id, 
            supportsAllDrives=True,
            fields="kind,id,name,mimeType,parents,webViewLink"
        ).execute()
        # print(response)
        return response
    except Exception as e:
        print(f'An error occurred: {e}')
        return None

def get_credentials(path_to_creds: str, scopes: list[str], user: str = None) -> service_account.Credentials:
    """
    Gets the credentials instance from a service account.

    Args:
        path_to_creds (str): The path and name of the credentials file to use
        scopes (list[str]): A list of scopes to use
        user (str): The email address of the user to impersonate in the case of domain wide delegation

    Returns:
        service_account.Credentials: The Credentials instance of the given service account

    Raises:
        Exception: If an error occured.
    """
    print("Getting credentials")
    try:
        credentials = service_account.Credentials.from_service_account_file(
            path_to_creds, 
            scopes=scopes
        )

        # If user is provided, impersonate that user (domain wide delegation)
        if user is not None:
            credentials = credentials.with_subject(user)

        return credentials
    except Exception as e:
        print(f"Failed to locate Application Credentials \n\n {e} \n\n Aborting")
        return None
    
def create_service(api_name: str, api_version: str, credentials: service_account.Credentials) -> Resource:
    """
    Initializes and returns a Google API service client.

    Args:
        api_name (str): The name of the API you want to interact with (e.g., 'sheets', 'drive').
        api_version (str): The version of the API to use (e.g., 'v3' for Drive API v3).
        credentials (Credentials): The authenticated credentials to access the Google API.

    Returns:
        service: A service object that allows you to interact with the chosen Google API.

    Raises:
        Exception: If an error occured.

    Example:
        service = create_service('drive', 'v3', credentials)
    """
    try:
        # Build the service
        service = build(api_name, api_version, credentials=credentials)
        return service
    except Exception as e:
        print(f'An error occurred: {e}')
        return None

def get_id_from_url(url: str) -> str:
    """
    Returns the ID for the Google File or Folder from the given URL.

    Args:
        url (str): The url of the Google File or Folder.

    Returns:
        id: The ID string for the given Google File or Folder.

    Example:
        id = get_id_from_url('https://docs.google.com/spreadsheets/d/<This part will be the id of the file or folder>/edit?gid=0')
    """
    match = re.search(r'[-\w]{25,}', url)
    if match:
        return match.group(0)
    return None
