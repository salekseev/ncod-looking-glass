import json
import os
import datetime
import boto3

from inspector import decimalencoder
from boto3.dynamodb.conditions import Key, Attr

now = datetime.datetime.now()
dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
# To go from low-level format to python
deserializer = boto3.dynamodb.types.TypeDeserializer()


def main():
    table = dynamodb.Table('transactions')

    result = table.query(
        IndexName="sysid-timestamp-index",
        ProjectionExpression="#timestamp, subsystem, payload",
        # Expression Attribute Names for Projection Expression only.
        ExpressionAttributeNames={"#timestamp": "timestamp"},
        KeyConditionExpression=Key('sysid').eq(
            'b3524e157e7d7bbca1859c4aeb7729cd'),
        ScanIndexForward=True
    )

    # create a response
    response = {
        "statusCode": 200,
        "body": json.dumps(result['Items'], cls=decimalencoder.DecimalEncoder)
    }

    return response


if __name__ == "__main__":
    print(main())
