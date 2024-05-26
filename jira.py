import requests
from requests.auth import HTTPBasicAuth
import json
import gmail as google_mail
import wget
import os

requests.packages.urllib3.disable_warnings()

url = "https://[YOUR-DOMAIN]/rest/api/2/search"

project = 'EUC Order Tracker'
type = 'Purchase Order'

#  https://[YOUR-DOMAIN]/browse/EUCOT-1234
ticket = 'EUCOT-1234'

# This query will get all the Jira tickets that is unresolved for the given project and type.
querystring = {"filter":"-1","jql":f"resolution = Unresolved and project = '{project}' and type = '{type}' ORDER BY createdDate DESC","fields":"id,key,displayName,description,updated,status,summary,customfield_11122,attachment"}

#  This query will get the info for a specific ticket. Mostly used for testing purposes.
# querystring = {"filter":"-1","jql":f"id = '{ticket}'  ORDER BY createdDate DESC","fields":"id,key,displayName,description,updated,status,summary,customfield_11122,attachment"}
# querystring = {"filter":"-1","jql":f"id = '{ticket}' ORDER BY createdDate DESC","fields":"*all"}

payload = ""
headers = {
    'Authorization': "Bearer [TOKEN-HERE]",
    'cache-control': "no-cache",
    'Postman-Token': "[TOKEN-HERE]",
}

# Makes the API call
response = requests.request("GET", url, data=payload, headers=headers, params=querystring)

# Gets the response as an JSON object
jiraitems = json.loads(response.text)
print(jiraitems)

# Initialises the Gmail service
gservice = google_mail.init_gmail()

# Loops through all the issues that have been returned from the API call
for issue in jiraitems['issues']:
    # print(issue)
    issueKey = issue['key']
    print(f'issueKey: {issueKey}')

    attachments = issue['fields']['attachment']
    # If there are attachments, create a folder and add them to it
    if len(attachments) > 0:
        # Check whether the specified path exists or not
        ticketPath = f'attachments/{issueKey}'
        # print(ticketPath)
        isExist = os.path.exists(ticketPath)
        if not isExist:
            # Create a new folder because it does not exist
            os.makedirs(ticketPath)
            print("The new directory is created!")

        # Get all the files in the ticket folder
        filesInDir = os.listdir(ticketPath)
        print(filesInDir)

        # Loops through all the attachments for the ticket
        for attachment in attachments:

            # Gets the information relevant for the attachment
            attachmentName = attachment['filename']
            attachmentUrl = attachment['content']
            attachmentType = attachment['mimeType']
            print(f'Attachment: {attachmentName} ({attachmentType})')
            # print(attachmentUrl)

            # If the attachment isn't already there, then download it
            if attachmentName not in filesInDir: 
                print(f'Downloading attachment: {attachmentName}')
                # Downloads the files into that directory
                attachment = wget.download(attachmentUrl, out = f'{ticketPath}/{attachmentName}', bar=None)
                print(f'{attachmentName} has been downloaded')

        # print(issue['fields']['summary'] +' - REMINDER')
        # print(issue['fields']['description'])
        # print(issue['fields']['customfield_11122']['displayName'])
        # print(issue['fields']['customfield_11122']['emailAddress'])

    # Sends an email notification for the Jira Issue
    emailTo = '[RECEIVER-EMAIL]'
    emailFrom = '[SENDER-EMAIL]'
    emailSubject = f"REMINDER - {issue['fields']['summary']}"
    emailBody =  f"{issue['fields']['description']}"
    if emailBody == '':
        emailBody = 'None provided'

    google_mail.gmail_send_message(gservice, emailTo, emailFrom, emailSubject, emailBody)