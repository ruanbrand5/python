import csv
import os
import re
import io
import time

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.discovery import Resource
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload, BatchHttpRequest
from googleapiclient.errors import HttpError

def test():
    # Replace these with your actual values
    path_to_creds = 'credentials.json'
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    user = '<Email Address for service account to impersonate>'

    folderId = "<Google Drive Folder ID here>"

    creds = get_credentials(path_to_creds, scopes, user)
    # credentials = get_credentials(path_to_creds, scopes)
    sheet_service = create_service('sheets', "v4", credentials=creds)

    drive_service = create_service('drive', "v3", credentials=creds)
    # spreadsheet = create_spreadsheet(drive_service, folderId, "test sheet")
    # print(spreadsheet)
    # print(create_spreadsheet.__doc__)

    query = f"'{folderId}' in parents and mimeType = 'application/vnd.google-apps.folder'"
    files = get_items_from_drive(drive_service, query)
    print(files)

def batch_update_parent(drive_service, batch_updates):
    """
    Updates the parents of multiple files in a batch using the Google Drive API.

    Args:
        drive_service: The authenticated Google Drive service object.
        batch_updates: A list of dictionaries, where each dictionary represents a file update and contains:
                       - 'removeParents': A list of parent IDs to remove (usually just the old parent ID).
                       - 'addParents': A list of parent IDs to add (usually just the new parent ID).
                       - 'fileId': The ID of the file to update.

    Returns:
        None. Prints success/failure messages for each file update.  Handles potential API errors.
    """

    batch = drive_service.new_batch_http_request(callback=process_response)
    for update in batch_updates:
        file_id = update['fileId']
        # body = {
        #     'addParents': update['addParents'],
        #     'removeParents': update['removeParents']
        # }

        # drive_service.files().update(
        #     supportsAllDrives=True,
        #     fileId=id,
        #     addParents=new_parent,
        #     removeParents=old_parent,
        #     fields='id, parents'
        # ).execute()

        request = drive_service.files().update(
            supportsAllDrives=True,
            fileId=file_id,
            addParents=update['addParents'],
            removeParents=update['removeParents'],
            fields='id, parents'
        )
        batch.add(request, request_id=file_id)

    try:
        execute_with_retries(batch)
    except Exception as e:
        print(f"Error executing batch request: {e}")

def execute_with_retries(batch, max_retries=3):
    for attempt in range(max_retries):
        try:
            batch.execute()
            return
        except HttpError as e:
            if e.resp.status in [500, 503]:
                wait_time = (2 ** attempt) + (0.5 * attempt)
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise
        # except TimeoutError:
        #     wait_time = (2 ** attempt) + (0.5 * attempt)
        #     print(f"Timeout occurred. Retrying in {wait_time} seconds...")
        #     time.sleep(wait_time)
        except Exception as e:
            print(f"Batch execution failed: {e}\nCode: {e.resp.status}")
            break  # Stop retrying for non-recoverable errors

def process_response(request_id, response, exception):
  """Callback function to handle the response from each batch request."""
  if exception is not None:
    print(f"Request ID: {request_id} - Error: {exception}")
  else:
      print(f"Request ID: {request_id} - File updated successfully: {response.get('id')}")

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

def upload_pdf_to_drive(drive_service: Resource, encrypted_stream, original_filename: str, parent_folder_id: str = None) -> dict:
    """
    Upload an encrypted PDF to Google Drive.

    Args:
        drive_service (Resource): The authenticated drive service instance
        encrypted_stream (str): The encrypted pdf file stream
        original_filename (str): The name of the file to save the stream as
        parent_folder_id (str): The id of the drive folder to save the file in

    Returns:
        file (dict): The newly created file in Google Drive

    Raises:
        Exception: If an error occured.
    """
    try:
        file_metadata = {
            "name": f"Encrypted_Stream_{original_filename}"
        }

        if parent_folder_id:
            file_metadata["parents"] = [parent_folder_id]
        
        media = MediaIoBaseUpload(encrypted_stream, mimetype="application/pdf", resumable=True)
        file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        
        return file
    except Exception as e:
        print(f'An error occurred: {e}')
        return None

def replicate_permissions(drive_service: Resource, from_drive_id: str, to_drive_id: str) -> str:
    """
    Replicate permissions from one drive to another.

    Args:
        drive_service (Resource): The authenticated drive service instance
        from_drive_id (str): The id of the drive to replicate its permissions
        to_drive_id (str): The id of the drive to have its permissions updated

    Raises:
        Exception: If an error occured.
    """
    from_permissions = get_permissions(drive_service, from_drive_id)
    print(f"Permissions to go through: {len(from_permissions)}")
    # print(f"from_permissions: \n{from_permissions}")

    to_permissions = get_permissions(drive_service, to_drive_id)
    # print(f"to_permissions: \n{to_permissions}")
    # return

    for permission in from_permissions:
        try:
            email = permission.get('emailAddress')
            print(f"Checking permissions for {email}")
            if email: 
                # Check if permission exists
                add_permission = True
                for to_permission in to_permissions:
                    if to_permission['type'] == "user":
                        if to_permission['emailAddress'] == email:
                            add_permission = False
                            break
                
                if add_permission:
                    print(f"Adding permission: {email} to {to_drive_id}")
                    drive_service.permissions().create(
                        fileId=to_drive_id,
                        body={
                            'type': permission['type'],
                            'role': permission['role'],
                            'emailAddress': email
                        },
                        sendNotificationEmail=False,
                        supportsAllDrives=True
                    ).execute()
        except Exception as e:
            print(f"Error replicating permission: {e}")
            
    return "Success"

def get_permissions(drive_service: Resource, folder_id: str) -> list[dict]:
    """
    Gets the permissions from a drive.

    Args:
        drive_service (Resource): The authenticated drive service instance
        folder_id (str): The id of the drive to return its permissions

    Returns:
        list[dict]: The list with all the permissions
        
    Raises:
        Exception: If an error occured.
    """
    try:
        permissions_list = []
        pageToken = None

        while True:
            request = drive_service.permissions().list(
                fileId=folder_id,
                supportsAllDrives=True,
                pageSize=100,
                pageToken=pageToken,
                fields="permissions(id,emailAddress,role,type,permissionDetails),nextPageToken"
            ).execute()

            permissions = request.get("permissions", [])
            permissions_list = permissions_list + permissions
            
            pageToken = request.get("nextPageToken", None)
            if not pageToken:
                break

        return permissions_list
    except Exception as e:
            print(f"Error replicating permission: {e}")

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

def move_file(drive_service: Resource, id: str, new_parent: str, old_parent: str) -> dict:
    """
    Update parents for a Google file/folder.
    
    Args:
        drive_service (Resource): The authenticated drive service instance
        id (str): The id of the file/folder to update
        new_parent (str): The id of the new parent folder
        old_parent (str): The id of the old parent folder

    Returns:
        dict: The File Resource of the Google file/folder

    Raises:
        Exception: If an error occured.
    """
    print(f"Updating the parent of: {id}")
    try:
        # Update the parent
        item = drive_service.files().update(
            supportsAllDrives=True,
            fileId=id,
            addParents=new_parent,
            removeParents=old_parent,
            fields='id, parents'
        ).execute()
        print(f"Updated file ID {id} to new parent ID {new_parent}")

        return item
    except Exception as e:
        print(f'An error occurred: {e}')
        return None
    
def update_parent_folder(drive_service: Resource, id: str, new_parent: str) -> dict:
    """
    Update parents for a Google file/folder.
    
    Args:
        drive_service (Resource): The authenticated drive service instance
        id (str): The id of the file/folder to update
        new_parent (str): The id of the new parent folder

    Returns:
        dict: The File Resource of the Google file/folder

    Raises:
        Exception: If an error occured.
    """
    print(f"Updating the parent of: {id}")
    try:
        # Get the current parent IDs
        file_metadata = drive_service.files().get(fileId=id, supportsAllDrives=True, fields='parents').execute()
        old_parents = ",".join(file_metadata.get('parents', []))

        # Update the parent
        item = drive_service.files().update(
            supportsAllDrives=True,
            fileId=id,
            addParents=new_parent,
            removeParents=old_parents,
            fields='id, parents'
        ).execute()
        print(f"Updated file ID {id} to new parent ID {new_parent}")

        return item
    except Exception as e:
        print(f'An error occurred: {e}')
        return None

def save_as_csv(sheet_service: Resource, sheet: dict, location: str) -> str:
    """
    Saves the current Google Sheet as a CSV file in the specified location

    Args:
        sheet_service (Resource): The authenticated sheet service instance
        sheet (dict): A dict representing a Google Sheet file
        location (str): The location where to save the CSV

    Returns:
        str: The File Resource of the newly created spreadsheet

    Raises:
        Exception: If an error occured.
    """
    print("Saving a Google Sheet as a CSV")
    try:
        # Retrieve data from the specified range
        result = sheet_service.spreadsheets().values().get(spreadsheetId=sheet.get('spreadsheetId'), range="Access").execute()
        values = result.get('values', [])

        if not values:
            print("No data found in the specified range.")
            return
        
        path = os.path.dirname(location)
        isExist = os.path.exists(path)
        if path and not isExist:
            # Create a new folder because it does not exist
            os.makedirs(path)
            print("The new directory is created!")

        # Write data to CSV
        with open(location, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(values)
            print(f"CSV file saved successfully at {location}")

        return "Success!"
    except Exception as e:
        print(f'An error occurred: {e}')
        return None

def get_spreadsheet(sheet_service: Resource, ss_id: str) -> dict:
    """
    Returns the spreadsheet with the specified id

    Args:
        sheet_service (Resource): The authenticated sheet service instance
        ss_id (str): The ID of the spreadsheet to return

    Returns:
        dict: The File Resource of the spreadsheet

    Raises:
        Exception: If an error occured.
    """
    print(f"Getting Google Sheet: {ss_id}")
    try:
        sheet = sheet_service.spreadsheets().get(
            spreadsheetId=ss_id, 
            fields='spreadsheetId,properties.title,sheets.properties.title,sheets.properties.sheetId,sheets.properties.index,sheets.properties.gridProperties'
        ).execute()
        return sheet
    except Exception as e:
        print(f'An error occurred: {e}')
        return None

def batch_get_values(sheet_service: Resource, ss_id: str, range_names: list[str]) -> dict:
  """
    Returns the data for the specified ranges of the Google Sheet.

    Args:
        sheet_service (Resource): The authenticated sheet service instance
        ss_id (str): The ID of the spreadsheet to return
        range_names (list[str]): A list containing all the ranges to retrieve

    Returns:
        dict: An object containing the values of the specified ranges

    Raises:
        Exception: If an error occured.
    """
  try:
    result = sheet_service.spreadsheets().values().batchGet(
        spreadsheetId=ss_id, 
        ranges=range_names
    ).execute()
    ranges = result.get("valueRanges", [])
    print(f"{len(ranges)} ranges retrieved")
    return ranges
  except Exception as error:
    print(f"An error occurred: {error}")
    return None
  
def get_items_from_drive(drive_service: Resource, query: str) -> list[dict]:
    """
    Queries the Google Drive Resource and return a list of items that match the given query

    Args:
        drive_service (Resource): The authenticated drive service instance
        query (str): A query for filtering the file results

    Returns:
        list[dict]: A list with all the File Resources that matched the given query

    Raises:
        Exception: If an error occured.
    """
    print("Getting items from Google Drive")
    try:
        items_list = []
        pageToken = None

        while True:
            request = drive_service.files().list(
                includeItemsFromAllDrives = True, 
                supportsAllDrives = True, 
                pageToken = pageToken,
                pageSize = 500,
                q = query, 
                fields = "files(id,name,mimeType,modifiedTime,createdTime,permissionIds,shortcutDetails),nextPageToken"
            ).execute()
            # print(request)
            items = request.get('files', [])
            items_list = items_list + items
            
            pageToken = request.get("nextPageToken", None)
            if not pageToken:
                break
            
        print(f"{len(items_list)} items matching the given query")
        return items_list
    except Exception as e:
        print(f'An error occurred: {e}')
        return []
    
def create_spreadsheet(drive_service: Resource, folder_id: str, sheet_name: str) -> dict:
    """
    Creates a new Google Sheet and returns its ID.

    Args:
        drive_service (Resource): The authenticated drive service instance
        folder_id (str): The ID of the parent folder where the spreadsheet will be created
        sheet_name (str): The name of the new spreadsheet

    Returns:
        dict: The File Resource of the newly created spreadsheet

    Raises:
        Exception: If an error occured.
    """
    print("Creating a new Google Sheet")
    try:
        sheet = drive_service.files().create(body={
            'name': sheet_name,
            'mimeType': 'application/vnd.google-apps.spreadsheet',
            "parents": [folder_id],
        }).execute()
        print(f"Sheet ID: {sheet.get('id')}")
        return sheet
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

if __name__ == "__main__":
    test()
