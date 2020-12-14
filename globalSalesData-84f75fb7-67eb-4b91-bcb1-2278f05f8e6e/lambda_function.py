import json
import urllib.parse
import psycopg2

print('Loading GlobalSalesData function')

dbname = ''
host = ''
port = ''
user = ''
password = ''
iam_role = ''

# This function will produce a series of reports (unload statements)
# used for comparing company totals to industry totals.

#

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    company_id = event['responsePayload']['company_id']

    #-------------------------------------
    # Define each unload  SQL
    # =-------------------------------------
    s3Folder = f's3://rseg176-filewarehouse/sales_reports_out/{company_id}/'
    file_info = "delimiter '|' HEADER \
    PARALLEL OFF \
    allowoverwrite"
    
    qryAvgIndSales =f"UNLOAD (' \
        select sum(case when COMPANY_NAME = ''{company_id}'' then total end) company_tot_sales, \
        sum(total) total_industry_sales, \
        avg(case when COMPANY_NAME = ''{company_id}'' then total end) company_avg_sales, \
        avg(total) Average_industry_sales \
        from SUPERMARKET_SALES sls \
        ') \
        to '{s3Folder}Avg_Industry_Sales.csv_' \
        credentials 'aws_iam_role={iam_role}' \
        {file_info};"
    
    qryProdCitySales =f"UNLOAD (' \
        select sls.PRODUCT_LINE, sls.CITY, \
        sum(case when COMPANY_NAME = ''{company_id}'' then total end) company_tot_sales, \
        sum(total) total_industry_sales, \
        avg(case when COMPANY_NAME = ''{company_id}'' then total end) company_avg_sales, \
        avg(total) Average_industry_sales \
        from SUPERMARKET_SALES sls \
        group by sls.PRODUCT_LINE, sls.CITY \
        order by 4 DESC, Sls.PRODUCT_LINE, sls.CITY \
        ') \
        to '{s3Folder}Ind_Product_City_Sales.csv_' \
        credentials 'aws_iam_role={iam_role}' \
        {file_info};"
    
    qryProdPaymentSales =f"UNLOAD (' \
        select sls.PRODUCT_LINE, sls.PAYMENT,  \
        sum(case when COMPANY_NAME = ''{company_id}'' then total end) company_tot_sales, \
        sum(total) total_industry_sales, \
        avg(case when COMPANY_NAME = ''{company_id}'' then total end) company_avg_sales, \
        avg(total) Average_industry_sales \
        from SUPERMARKET_SALES sls \
        group by sls.PRODUCT_LINE, sls.PAYMENT \
        order by 4 DESC, Sls.PRODUCT_LINE, sls.PAYMENT \
        ') \
        to '{s3Folder}Ind_Product_Payment_Sales.csv_' \
        credentials 'aws_iam_role={iam_role}' \
        {file_info};"
        
    qryProductSales =f"UNLOAD (' \
        select sls.PRODUCT_LINE,  \
        sum(case when COMPANY_NAME = ''{company_id}'' then total end) company_tot_sales, \
        sum(total) total_industry_sales, \
        avg(case when COMPANY_NAME = ''{company_id}'' then total end) company_avg_sales, \
        avg(total) Average_industry_sales \
        from SUPERMARKET_SALES sls \
        group by sls.PRODUCT_LINE \
        order by 3 DESC, Sls.PRODUCT_LINE \
        ') \
        to '{s3Folder}Ind_Product_Sales.csv_' \
        credentials 'aws_iam_role={iam_role}' \
        {file_info};"

    try:
        con = psycopg2.connect(dbname=dbname, host=host,
                               port=port, user=user, password=password)
        cur = con.cursor()
        print ("Executing qryAvgIndSales")
        cur.execute(qryAvgIndSales)
        print ("Executing qryProdCitySales")        
        cur.execute(qryProdCitySales)
        print ("Executing qryProdPaymentSales")           
        cur.execute(qryProdPaymentSales)
        print ("Executing qryProductSales")           
        cur.execute(qryProductSales)
        con.commit()
        cur.close()
        con.close()

    except Exception as e:
        print(e)
        print('Error generating global sales reports')
        raise e

    print('COMPLETE')
    return {'company_id': company_id}