import json
import urllib.parse
import psycopg2
import boto3

print('Loading function')

dbname = ''
host = ''
cluster_id = ''
port = ''
user = ''
iam_role = ''

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    message = json.loads(event['Records'][0]['Sns']['Message'])
    company_id = message['responsePayload']['company_id']
    
    unload_query=f"UNLOAD (' \
    select sls.PRODUCT_LINE, sls.BRANCH_CODE, sum(total) total_sales \
    FROM SUPERMARKET_SALES sls  \
    WHERE sls.COMPANY_NAME = ''{company_id}'' \
    group by sls.PRODUCT_LINE, sls.BRANCH_CODE \
    order by 3 desc \
    ') \
    to 's3://rseg176-filewarehouse/sales_reports_out/{company_id}/Product_Branch_Sales.csv_' \
    credentials 'aws_iam_role={iam_role}' \
    CSV delimiter ',' HEADER PARALLEL OFF allowoverwrite;"

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
        cur.execute(unload_query)
        con.commit()
        cur.close() 
        con.close()
        
    except Exception as e:
        print(e)
        print('Error generating branch sales reports')
        raise e

    print('COMPLETE')
    return {'company_id':company_id}
