import json
import urllib.parse
import boto3
import psycopg2
import time
import random
import string

print('Loading function')

dbname = ''
host = ''
cluster_id = ''
port = ''
user = ''
iam_role = ''
s3 = boto3.client('s3')
backup_bucket = "rseg176-backup"

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    con = None
    cur = None
    table_name=None
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    company_id = key.split('/')[1]
    
    print("KEY: "+key)
    print("BUCKET: "+bucket)
    print("COMPANY: "+company_id)
    
    try:
        client = boto3.client('redshift')
        cluster_creds = client.get_cluster_credentials(DbUser=user,
                                                       DbName=dbname,
                                                       ClusterIdentifier=cluster_id,
                                                       AutoCreate=False)
        con = psycopg2.connect(dbname=dbname, host=host,
                               port=port, user=cluster_creds['DbUser'], 
                               password=cluster_creds['DbPassword'])
        cur = con.cursor()
        print("connected to redshift")
        
    except Exception as e:
        print(e)
        print('Error connecting to Redshift.')
        raise e
        
    try:
        # generate random string lowercase
        letters = string.ascii_lowercase
        table_name=f'temp_{company_id}_sales_' + ''.join(random.choice(letters) for i in range(10))
        print(table_name)
        create_table_command=f"CREATE TABLE {table_name} ( \
          INVOICE_ID    VARCHAR(15)    NOT NULL, \
          BRANCH_CODE   VARCHAR(3), \
          CITY          VARCHAR(15), \
          CUSTOMER_TYPE VARCHAR(10), \
          GENDER        VARCHAR(10), \
          PRODUCT_LINE  VARCHAR(40), \
          UNIT_PRICE    VARCHAR(7), \
          QUANTITY      VARCHAR(5), \
          TAX           VARCHAR(7), \
          TOTAL         VARCHAR(8), \
          SALES_DATE    VARCHAR(10), \
          SALES_TIME    VARCHAR(10), \
          PAYMENT                  VARCHAR(15), \
          COGS                     VARCHAR(10), \
          GROSS_MARGIN_PERCENTAGE  VARCHAR(15), \
          GROSS_INCOME             VARCHAR(10), \
          RATING                   VARCHAR(5) \
        );"
        cur.execute(create_table_command)

    except Exception as e:
        print(e)
        print(f'Error creating temp table.')
        raise e

    try:
        copy_command= f"copy {table_name} from \
        's3://{bucket}/{key}' \
        iam_role '{iam_role}' \
        delimiter ',' IGNOREHEADER 1"
        cur.execute(copy_command)
        con.commit()
        cur.close() 
        con.close()
        
    except Exception as e:
        print(e)
        print(f'Error copying csv "{key}" to table.')
        raise e
    
    try:
        print('BACKING UP SALES DATA')
        millis = int(round(time.time() * 1000))
        backup_file=f"sales-backups/{company_id}/{company_id}_sales_{millis}.csv"
        
        # Copy object A as object B
        s3.copy_object(Bucket=backup_bucket, Key=backup_file, CopySource=f"{bucket}/{key}")
         
        # Delete the former object A
        s3.delete_object(Bucket=bucket,Key=key)
        
    except Exception as e:
        print(e)
        print(f'Error backing up csv "{key}" to "{backup_file}"".')
        raise e
    
    print('COMPLETE')
    return {'company_id':company_id, 'table_name': table_name}
