import json
import uuid
import boto3

AWS_BUCKET_NAME = 'air-quality-live'
COUNTRY = ["CN", "AU", "JP", "VN", "US"]

def save_to_bucket(bucket_name, data):
    print(data)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    path = str(uuid.uuid4())
    bucket.put_object(
        ACL='public-read',
        ContentType='application/json',
        Key=path,
        Body=data,
    )

    body = {
        "uploaded": "true",
        "bucket": bucket_name,
        "path": path,
    }
    return {
        "statusCode": 200,
        "body": json.dumps(body)
    }

def lambda_handler(event, context):
    record = event["Records"][0]["Sns"]["MessageAttributes"]
    fields = ["country", "city", "date_utc", "value", "location", "latitude", "longitude"]
    data = ""
    for f in fields: 
        data = data + f"{record[f]['Value']}\t"
    data = data[:-1]
    if record["country"]["Value"] in COUNTRY:
        save_to_bucket(AWS_BUCKET_NAME, data)

    return {'statusCode': 200}
