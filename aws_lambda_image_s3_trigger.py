import boto3
from urllib.parse import unquote_plus

def label_function(bucket, name):
    """This takes an S3 bucket and a image name!"""
    print(f"This is the bucketname {bucket} !")
    print(f"This is the imagename {name} !")
    rekognition = boto3.client("rekognition")
    response = rekognition.detect_labels(
        Image={
            "S3Object": {
                "Bucket": bucket,
                "Name": name,
            }
        },
    )
    labels = response["Labels"]
    print(f"I found these labels {labels}")
    return labels


def add_to_dynamoDB(labels):
    dynamodb = boto3.client("dynamodb")
    for label in labels:
        add_to_db = dynamodb.put_item(
            TableName = 'image_labels',
            Item = {
                'Name' : {'S' : str(label['Name'])},
                'confidence' : {'N' : str(label['Confidence'])},
            }
            )
    print('Sucessfully added data to DynamoDB')
    return labels
    
def lambda_handler(event, context):
    """This is a computer vision lambda handler"""

    print(f"This is my S3 event {event}")
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        print(f"This is my bucket {bucket}")
        key = unquote_plus(record["s3"]["object"]["key"])
        print(f"This is my key {key}")

    my_labels = label_function(bucket=bucket, name=key)
    add_to_dynamoDB(labels=my_labels)
    return my_labels
