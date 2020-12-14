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
    for user in event['users']:

        user_name=user['name']
        password=user['password']
        in_folder_name = f"sales_records_in/{user_name}"
        out_folder_name = f"sales_reports_out/{user_name}"
    
        try:
            user = iam_client.create_user(UserName=user_name)
        except ClientError as error:
            if error.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f'User already exists {user_name}')
                continue
            else:
                print(f'Unexpected error occured while creating user {user_name}\n', error)
                continue
            
        try:
            login_profile = iam_client.create_login_profile(
                UserName=user_name,
                Password=password,
                PasswordResetRequired=True
            )
        except ClientError as error:
            if error.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f'login profile already exists {user_name}')
            else:
                print(f'Unexpected error occured while creating login profilefor {user_name}\n', error)
                continue
        print('User with UserName:{0} got created successfully'.format(user_name))
    
        # Add user to group
        add_user_to_group_res = iam_client.add_user_to_group(
            GroupName=group_name,
            UserName=user_name
        )
        try:
            s3.put_object(Bucket=bucket_name, Key=(in_folder_name+'/'))
            s3.put_object(Bucket=bucket_name, Key=(out_folder_name+'/'))
        except Exception as error:
            print(f'Failed to create folders for user {user_name}\n', error)
            continue
    
    print('COMPLETE')
