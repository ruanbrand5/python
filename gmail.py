from __future__ import print_function

import os.path
import os
import base64
import mimetypes

from email.message import EmailMessage
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText

import google.auth

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# This is constructed from 
#  - https://developers.google.com/gmail/api/quickstart/python
#  - https://developers.google.com/gmail/api/guides/sending#python

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.compose']

def init_gmail():
    """Shows basic usage of the Gmail API.
    Lists the user's Gmail labels.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('gmailtoken.json'):
        creds = Credentials.from_authorized_user_file('gmailtoken.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('gmailtoken.json', 'w') as token:
            token.write(creds.to_json())

    try:
        # Call the Gmail API
        service = build('gmail', 'v1', credentials=creds)
        
        return service

    except HttpError as error:
        # TODO(developer) - Handle errors from gmail API.
        print(f'An error occurred: {error}')


def gmail_create_draft_with_attachment(service,mail_to: str,mail_from: str,mail_subject:str,mail_message: str,attachment_filename: str,attachment_filename_path: str):
    
    try:
        mime_message = EmailMessage()

        # headers
        mime_message['To'] = mail_to
        mime_message['From'] = mail_from
        mime_message['Subject'] = mail_subject

        # text
        mime_message.set_content(
                mail_message
        )

        # guessing the MIME type
        type_subtype, _ = mimetypes.guess_type(attachment_filename_path + attachment_filename)
        maintype, subtype = type_subtype.split('/')

        with open(attachment_filename_path + attachment_filename, 'rb') as fp:
            attachment_data = fp.read()
        mime_message.add_attachment(attachment_data, maintype, subtype,filename=attachment_filename)

        encoded_message = base64.urlsafe_b64encode(mime_message.as_bytes()).decode()

        create_draft_request_body = {
            'message': {
                'raw': encoded_message
            }
        }
        # pylint: disable=E1101
        draft = service.users().drafts().create(userId="me",
                                                body=create_draft_request_body)\
            .execute()
        
        draft_id = draft["id"]

        print(F'Draft id: {draft["id"]}\nDraft message: {draft["message"]}')
        
        service.users().drafts().send(userId='me', body={ 'id': draft_id }).execute()

    except HttpError as error:
        print(F'An error occurred: {error}')
        draft = None
    return draft


def build_file_part(file):
    """Creates a MIME part for a file.

    Args:
      file: The path to the file to be attached.

    Returns:
      A MIME part that can be attached to a message.
    """
    content_type, encoding = mimetypes.guess_type(file)

    if content_type is None or encoding is not None:
        content_type = 'application/octet-stream'
    main_type, sub_type = content_type.split('/', 1)
    if main_type == 'text':
        with open(file, 'rb'):
            msg = MIMEText('r', _subtype=sub_type)
    elif main_type == 'image':
        with open(file, 'rb'):
            msg = MIMEImage('r', _subtype=sub_type)
    elif main_type == 'audio':
        with open(file, 'rb'):
            msg = MIMEAudio('r', _subtype=sub_type)
    else:
        with open(file, 'rb'):
            msg = MIMEBase(main_type, sub_type)
            msg.set_payload(file.read())
    filename = os.path.basename(file)
    msg.add_header('Content-Disposition', 'attachment', filename=filename)
    return msg

def gmail_send_message(service,mail_to: str,mail_from: str,mail_subject:str,mail_message: str):
    try:
    
        message = EmailMessage()

        message.set_content(mail_message)

        message['To'] = mail_to
        message['From'] = mail_from
        message['Subject'] = mail_subject

        # encoded message
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()) \
            .decode()

        create_message = {
            'raw': encoded_message
        }
        # pylint: disable=E1101
        send_message = (service.users().messages().send
                        (userId="me", body=create_message).execute())
        print(F'Message Id: {send_message["id"]}')
    except HttpError as error:
        print(F'An error occurred: {error}')
        send_message = None
    return send_message