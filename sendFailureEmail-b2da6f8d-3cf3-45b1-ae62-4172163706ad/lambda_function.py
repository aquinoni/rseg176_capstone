import json
import boto3

s3_file = None
email_body = None

def find(key, dictionary):
    for k, v in dictionary.items():
        if k == key:
            return v
        elif isinstance(v, dict):
            result = find(key, v)
            if result is not None:
                return result
        elif isinstance(v, list):
            for d in v:
                if isinstance(d, dict):
                    result = find(key, d)
                    if result is not None:
                        return result
                        
def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    # Create an SNS client
    sns = boto3.client('sns')
    
    failed_lambda = event['requestContext']['functionArn'].split(':')[6]
    email_body = f'There was an error while executing Pipeline.\n\n'
    email_body += f'Lambda: {failed_lambda}\n'
    
    # Find s3 info
    s3_info = find('s3', event)
    if s3_info is not None:
        s3_file = f's3://{s3_info["bucket"]["name"]}/{s3_info["object"]["key"]}'
        company_id = s3_info['object']['key'].split('/')[1]
        email_body += f'Company: {company_id}\n'
        email_body += f'Trigger File: {s3_file}\n\n'
    
    # Find error info
    error_message = find('errorMessage', event)
    error_type = find('errorType', event)
    stack_trace = find('stackTrace', event)
    
    if error_type is not None:
        email_body += f'Error Type: {error_type}.\n'
    if error_message is not None:
        email_body += f'Error Message: "{error_message}".\n'
    if stack_trace is not None:
        stack_trace = "\n".join(stack_trace)
        email_body += f'Stack Trace: \n{stack_trace}\n'
    
    email_body += '\n--------------------------------------------------------\n'
    email_body += json.dumps(event, indent=2)

    # Publish a simple message to the specified SNS topic
    response = sns.publish(
        TopicArn='arn:aws:sns:us-east-1:139454215296:PipelineFailedErrorAlert',    
        Message=email_body   
    )
    
    # Print out the response
    print(response)