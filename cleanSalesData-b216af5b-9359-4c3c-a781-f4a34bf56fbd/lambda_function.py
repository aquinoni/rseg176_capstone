import json
import urllib.parse
import psycopg2
import boto3

print('Loading function')

dbname = 'dev'
host = 'examplecluster.c5xajgza5xzb.us-east-1.redshift.amazonaws.com'
cluster_id = 'examplecluster'
port = '5439'
user = 'awsuser'

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    company_id = event['responsePayload']['company_id']
    table_name = event['responsePayload']['table_name']

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
        
        clean_query=f"INSERT INTO supermarket_sales \
        SELECT '{company_id}', \
              INVOICE_ID, \
              BRANCH_CODE, \
              CITY, \
              CUSTOMER_TYPE, \
              GENDER, \
              PRODUCT_LINE, \
              TO_NUMBER (UNIT_PRICE, '9999999.99'), \
              QUANTITY::integer, \
              TO_NUMBER (TAX, '9999999.99999'), \
              TO_NUMBER (TOTAL, '9999999.99999'), \
              TO_DATE (SALES_DATE, 'MM/DD/YYYY'), \
              SALES_TIME, \
              PAYMENT, \
              TO_NUMBER (COGS, '9999999.99'), \
              TO_NUMBER (GROSS_MARGIN_PERCENTAGE, '9999999.999999999'), \
              TO_NUMBER (GROSS_INCOME, '9999999.99999'), \
              TO_NUMBER (RATING, '9999999.99') \
          FROM {table_name} sls_in \
          WHERE SLS_in.INVOICE_ID NOT IN (SELECT SLS_OLD.INVOICE_ID FROM SUPERMARKET_SALES SLS_OLD where company_name = '{company_id}')"
        
        cur.execute(clean_query)
        
        drop_table=f"DROP TABLE {table_name}"
        cur.execute(drop_table)

        con.commit()
        cur.close() 
        con.close()
        
    except Exception as e:
        print(e)
        print('Error running clean_query command')
        raise e

    print('COMPLETE')
    return {'company_id':company_id}