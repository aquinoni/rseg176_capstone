import json
import boto3
import time
import random
import string
from botocore.exceptions import ClientError


print('Loading function')

iam_client = boto3.client('iam')
s3 = boto3.client('s3')

group_name='S3BucketFoldersPerUser'
bucket_name = "rseg176-filewarehouse"


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    user_response = []
    for user in json.loads(event['body'])['users']:

        user_name=user['name']
        password=user['password']
        in_folder_name = f"sales_records_in/{user_name}"
        out_folder_name = f"sales_reports_out/{user_name}"
        status = "Created successfully"
        print(f"creating user {user_name}")
        
        try:
            try:
                user = iam_client.create_user(UserName=user_name)
            except ClientError as error:
                if error.response['Error']['Code'] == 'EntityAlreadyExists':
                    status = f'User already exists'
                    print(f'{status}: {user_name}\n', error)
                    continue
                else:
                    status = f'Unexpected error occured while creating user'
                    print(f'{status}: {user_name}\n', error)
                    continue

            try:
                login_profile = iam_client.create_login_profile(
                    UserName=user_name,
                    Password=password,
                    PasswordResetRequired=True
                )
            except ClientError as error:
                if error.response['Error']['Code'] == 'EntityAlreadyExists':
                    status = f'Login profile already exists'
                    print(f'{status}: {user_name}\n', error)
                    continue
                else:
                    status = f'Unexpected error occured while creating login profile'
                    print(f'{status}: {user_name}\n', error)
                    continue

            print(f'User {user_name} created successfully')
    
            try:
                add_user_to_group_res = iam_client.add_user_to_group(
                    GroupName=group_name,
                    UserName=user_name
                )
            except Exception as error:
                status = f'Failed to add user to group'
                print(f'{status}: {user_name}\n', error)
                continue

            try:
                s3.put_object(Bucket=bucket_name, Key=(in_folder_name+'/'))
                s3.put_object(Bucket=bucket_name, Key=(out_folder_name+'/'))
            except Exception as error:
                status = f'Failed to create folders for user'
                print(f'{status}: {user_name}\n', error)
                continue
        finally:
            print({"name":user_name,"status":status} )
            user_response.append({"name":user_name,"status":status})
            print(user_response)
    
    print('COMPLETE')
    print(user_response)
    response = {
        "statusCode": 200,
        "body": json.dumps(user_response),
        "isBase64Encoded": False
    }
    return response;
