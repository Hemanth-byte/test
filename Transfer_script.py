from __future__ import print_function
import boto3
import io
import datetime
import json
import time
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools

s3 = boto3.resource('s3')
google_access_key_id = "GOOGOAVYADDK6LWOXS2YV6RD"
google_access_key_secret = "6+rk54M3uayrQZwvoEaeufvmSe8Xxey1YkH6SKsC"
SCOPES = ['https://www.googleapis.com/auth/ediscovery']


def list_exports(service, matter_id):
    return service.matters().exports().list(matterId=matter_id).execute()


def get_export_by_id(service, matter_id, export_id):
    return service.matters().exports().get(matterId=matter_id, exportId=export_id).execute()


def get_service():
    """
    Look for an active credential token, if one does not exist, use credentials.json
    and ask user for permission to access.  Store new token, return the service object
    """
    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('credentials.json', SCOPES[1])
        creds = tools.run_flow(flow, store)
    service = build('vault', 'v1', http=creds.authorize(Http()))

    return service


def create_drive_export(service, matter_id, export_name, user_lis):
    """
    once we have a matter_id , we can create an export under it with the relevant files we are looking for.

    """
    today = datetime.datetime.now()
    print("creating a drive export at {}".format(today))
    drive_query_options = {'includeTeamDrives': True}
    drive_query = {
        'corpus': 'DRIVE',
        'dataScope': 'ALL_DATA',
        'searchMethod': 'ACCOUNT',
        'accountInfo': {
            'emails': user_lis
        },
        'driveOptions': drive_query_options,
        'endTime': '{}-{}-{}T00:00:00Z'.format(today.year, today.month, today.day),
        'startTime': '2000-01-01T00:00:00Z',
        'timeZone': 'Etc/GMT'
    }

    wanted_export = {
        'name': export_name,
        'query': drive_query,
        'exportOptions': {
            'driveOptions': {}
        }
    }

    return service.matters().exports().create(matterId=matter_id, body=wanted_export).execute()


def create_mail_export(service, matter_id, export_name, user_lis):
    """
    once we have a matter_id , we can create an export under it with the relevant files we are looking for.
    """
    today = datetime.datetime.now()
    print("creating a mail export at {}".format(today))
    mail_query = {
        'corpus': 'MAIL',
        'dataScope': 'ALL_DATA',
        'searchMethod': 'ACCOUNT',
        'accountInfo': {
            'emails': user_lis
        },
        'endTime': '{}-{}-{}T00:00:00Z'.format(today.year, today.month, today.day),
        'startTime': '2000-01-01T00:00:00Z',
        'timeZone': 'Etc/GMT'
    }
    export_options = {
        "mailOptions": {
            "exportFormat": "PST"
        },
    }
    wanted_export = {
        'name': export_name,
        'exportOptions': export_options,
        'query': mail_query,
    }

    return service.matters().exports().create(matterId=matter_id, body=wanted_export).execute()


def get_export(service, matter_id, export_id):
    return service.matters().exports().get(matterId=matter_id, exportId=export_id).execute()


def get_gcs_objects(google_access_id, google_access_secret, gc_bucket_name, gc_object_name):
    client1 = boto3.client("s3", region_name="auto",
                           endpoint_url="https://storage.googleapis.com",
                           aws_access_key_id=google_access_id,
                           aws_secret_access_key=google_access_secret)
    object1 = s3.Object('gcloudtos3test', gc_object_name)
    f = io.BytesIO()
    client1.download_fileobj(gc_bucket_name, gc_object_name, f)
    object1.put(Body=f.getvalue())


def etag_compare(etag, md5hash):
    et = etag[1:-1]
    if '-' in et and et == md5hash:
        return True
    if '-' not in et and et == md5hash:
        return True
    return False


def main():
    service = get_service()
    user_list = ['hemanth@wearemiq.com']
    matter_id = '0b1d46e6-d970-4718-9483-24a1ed2564b3'
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    export = create_drive_export(service, matter_id, user_list[0] + "export_drive.{}".format(timestamp), user_list)
    while export['status'] == 'IN_PROGRESS':
        export = get_export(service, matter_id, export['id'])
        time.sleep(5)
    print(json.dumps(export['cloudStorageSink']['files'], indent=2))
    export2 = create_mail_export(service, matter_id, user_list[0] + "export_mail.{}".format(timestamp), user_list)
    while export2['status'] == 'IN_PROGRESS':
        export2 = get_export(service, matter_id, export2['id'])
        time.sleep(5)
    print(json.dumps(export2['cloudStorageSink']['files'], indent=2))
    temp = export['cloudStorageSink']['files']
    for i in range(len(temp)):
        repeat = False
        while not repeat:
            get_gcs_objects(google_access_key_id, google_access_key_secret, temp[i]['bucketName'],
                            temp[i]['objectName'])
            obj = boto3.resource("s3").Object("gcloudtos3test", temp[i]['objectName'])
            repeat = etag_compare(obj.e_tag, temp[i]['md5Hash'])
    temp = export2['cloudStorageSink']['files']
    for i in range(len(temp)):
        repeat = False
        while not repeat:
            get_gcs_objects(google_access_key_id, google_access_key_secret, temp[i]['bucketName'],
                            temp[i]['objectName'])
            obj = boto3.resource("s3").Object("gcloudtos3test", temp[i]['objectName'])
            repeat = etag_compare(obj.e_tag, temp[i]['md5Hash'])


if __name__ == '__main__':
    main()
