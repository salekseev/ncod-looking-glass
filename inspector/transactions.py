import json
import os
import datetime
import boto3

from inspector import decimalencoder
from boto3.dynamodb.conditions import Key, Attr

now = datetime.datetime.now()
dynamodb = boto3.resource('dynamodb')


def get(event, context):
    table = dynamodb.Table(os.environ['DYNAMODB_TABLE_TRANSACTIONS'])

    result = table.query(
        IndexName="sysid-timestamp-index",
        ProjectionExpression="#timestamp, subsystem, payload",
        # Expression Attribute Names for Projection Expression only.
        ExpressionAttributeNames={"#timestamp": "timestamp"},
        KeyConditionExpression=Key('sysid').eq(event['pathParameters']['id']),
        ScanIndexForward=False
    )

    new_items = []

    for item in result['Items']:
        new_item = {
            'timestamp': datetime.datetime.utcfromtimestamp(item['timestamp']).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'subsystem': item['subsystem'],
            'payload': item['payload']
        }
        new_items.append(new_item)

    # create a response
    response = {
        "statusCode": 200,
        "body": json.dumps(new_items, indent=2)
    }

    return response
