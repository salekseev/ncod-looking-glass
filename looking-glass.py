import json
import boto3
import os
import datetime
import decimal
from boto3.dynamodb.conditions import Key, Attr


now = datetime.datetime.now()

session = boto3.Session()

dynamodb_client = session.resource('dynamodb')
ssm_client = session.client('ssm')


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


def fetch_page(table, fetch_args):
    response = table.scan(**fetch_args)

    return response


def fetch_data(table):
    data = []
    call_args = {}

    while True:
        response = fetch_page(table, call_args)
        data.extend(response.get('Items', []))

        call_args['ExclusiveStartKey'] = response.get('LastEvaluatedKey')
        if call_args['ExclusiveStartKey'] is None:
            break

    return data


def fetch_transactions(sysid):
    table = dynamodb_client.Table('transactions')

    response = table.query(
        ProjectionExpression="#year_month, timestamp, payload, subsystem, sysid",
        # Expression Attribute Names for Projection Expression only.
        ExpressionAttributeNames={"#year_month": "year_month"},
        KeyConditionExpression=Key('year').eq('{year}_{month}'.format(
            year=now.year, month=now.month)) & Key('sysid').eq(sysid),
        ScanIndexForward=True
    )

    return response[u'Items']


def hello(event, context):
    body = fetch_transactions('9784233fdbed57c0bb2343a84b96193e')

    response = {
        "statusCode": 200,
        "body": json.dumps(body, cls=DecimalEncoder)
    }

    return response
