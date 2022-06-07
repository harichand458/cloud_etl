import json
import urllib.parse
import boto3

print('Loading function')
s3 = boto3.client('s3')


def save_job_categorize_details(email_address, relavant_flag):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('gmail_job_categories')
    item = {
        'job_id': 'gmail_categorize',
        'email_address': email_address,
        'is_relevant': relavant_flag
    }
    table.put_item(Item=item)


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])

        file_contents = response['Body'].read().decode('utf-8')
        file_records = file_contents.splitlines()

        for msg in file_records:
            msg_dict = json.loads(msg)
            for header in msg_dict['payload']['headers']:
                if header['name'] == 'From':
                    print(f"From:  {header['value']}")
                    email_address = header['value']
                if header['name'] == 'Subject':
                    print(f"Subject: {header['value']}")
                    relavant_flag = "data" in header['value'].lower()
            print(f"----Relavant : ---{relavant_flag}--------------------")
            save_job_categorize_details(email_address, relavant_flag)

        return response['ContentType']
    except Exception as e:
        print(e)
        print(
            'Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(
                key, bucket))
        raise e



