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
        ScanIndexForward=True
    )

    # create a response
    response = {
        "statusCode": 200,
        "body": json.dumps(result['Items'], cls=decimalencoder.DecimalEncoder, indent=4, sort_keys=True)
    }

    return response
