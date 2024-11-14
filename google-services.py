from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.discovery import Resource

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

def get_items_from_drive(drive_service: Resource, query: str) -> list[dict]:
    """
    """
    print("Getting items from Drive")
    try:
        response = drive_service.files().list(
            includeItemsFromAllDrives = True, 
            supportsAllDrives = True, 
            pageToken = "",
            pageSize = 500,
            q = query, 
            fields = "files(id,name,modifiedTime,createdTime),nextPageToken"
        ).execute()
        print(response)
        items = response.get('files', [])
        return items
    except Exception as e:
        print(f'An error occurred: {e}')
        return []
    
def create_spreadsheet(drive_service: Resource, folder_id: str, sheet_name: str) -> str:
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

if __name__ == "__main__":
    test()