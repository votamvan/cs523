import json
import uuid
import boto3
from datetime import datetime

AWS_BUCKET_NAME = 'air-quality-live'
COUNTRY = ["CN", "IN", "US", "ES", "FR", "PL", "TW", "GB", "CZ", "CL", "NL", "AU", "CA", "PT", "SK", "TH"]

def save_to_bucket(bucket_name, country, city, data):
    print(data)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    path = country + "-" + city + "-" + datetime.now().strftime("%Y%m%d-%H%M%S")
    bucket.put_object(
        ACL='public-read',
        ContentType='text/html',
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
    print(json.dumps(event))
    record = event["Records"][0]["Sns"]["MessageAttributes"]
    fields = ["country", "city", "date_utc", "value", "location", "latitude", "longitude"]
    data = ""
    for f in fields:
        data = data + f"{record[f]['Value']}\t"
    data = data[:-1]
    country = record["country"]["Value"]
    city = record["city"]["Value"]
    # if country in COUNTRY:
    if country == "US":
        save_to_bucket(AWS_BUCKET_NAME, country, city, data)

    return {'statusCode': 200}
