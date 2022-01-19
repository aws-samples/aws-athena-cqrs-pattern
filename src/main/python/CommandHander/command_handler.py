#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
#vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import sys
import os
import json
import logging
import math
import datetime
from urllib.parse import urlparse

import boto3
import botocore

LOGGER = logging.getLogger()
if len(LOGGER.handlers) > 0:
  # The Lambda environment pre-configures a handler logging to stderr.
  # If a handler is already configured, `.basicConfig` does not execute.
  # Thus we set the level directly.
  LOGGER.setLevel(logging.INFO)
else:
  logging.basicConfig(level=logging.INFO)


AWS_REGION_NAME = os.getenv('REGION_NAME', 'us-east-1')
ATHENA_QUERY_OUTPUT_BUCKET_NAME = os.getenv('ATHENA_QUERY_OUTPUT_BUCKET_NAME')
ATHENA_WORK_GROUP_NAME = os.getenv('ATHENA_WORK_GROUP_NAME', 'primary')
DDB_TABLE_NAME = os.getenv('DDB_TABLE_NAME')

def lambda_handler(event, context):
  LOGGER.info(event)

  http_method = event['httpMethod']
  if http_method != 'POST':
    response = {
      'statusCode': 405,
      'body': json.dumps({'error': 'mehtod not allowed'}),
      'isBase64Encoded': False
    }
    return response

  query = json.loads(event['body'])
  query_output_location = query['ResultConfiguration']['OutputLocation']
  url_parse_result = urlparse(query_output_location, scheme='s3')
  s3_bucket_name = url_parse_result.netloc
  if s3_bucket_name != ATHENA_QUERY_OUTPUT_BUCKET_NAME:
    response = {
      'statusCode': 400,
      'body': json.dumps({'error': 'invalid output_location'}),
      'isBase64Encoded': False
    }
    return response

  athena_work_group = query.get('WorkGroup', ATHENA_WORK_GROUP_NAME)
  if athena_work_group != ATHENA_WORK_GROUP_NAME:
    response = {
      'statusCode': 400,
      'body': json.dumps({'error': 'invalid athena work group'}),
      'isBase64Encoded': False
    }
    return response

  req_user_id = event['queryStringParameters']['user']

  athena_client = boto3.client('athena', region_name=AWS_REGION_NAME)
  try:
    response = athena_client.start_query_execution(**query)
    query_execution_id = response['QueryExecutionId']
    LOGGER.info('QueryExecutionId: %s' % query_execution_id)

    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION_NAME)
    ddb_table = dynamodb.Table(DDB_TABLE_NAME)

    expired_date = datetime.datetime.utcnow() + datetime.timedelta(days=7)
    #TODO: should handle ProvisionedThroughputExceededException
    ddb_table.put_item(Item={
      'user_id': req_user_id,
      'query_id': query_execution_id,
      'query_status': 'QUEUED',
      #XXX: The TTL attribute’s value must be a timestamp in Unix epoch time format in seconds.
      'expired_at': math.ceil(expired_date.timestamp())
    })

    response = {
      'statusCode': 200,
      'body': json.dumps(response),
      'isBase64Encoded': False
    }
  except Exception as ex:
    response = {
      'statusCode': 500,
      'body': repr(ex),
      'isBase64Encoded': False
    }
  return response


if __name__ == '__main__':
  import argparse

  parser = argparse.ArgumentParser()
  parser.add_argument('--region-name', default='us-east-1',
    help='aws region name: default=us-east-1')
  parser.add_argument('--output-location', required=True,
    help='aws athena query output location. ex) s3://bucket-name/path/to/object')
  parser.add_argument('--work-group-name', default='primary',
    help='aws athena work group name: default=primary')
  parser.add_argument('--print-query-string', action='store_true',
    help='print aws athena query string')
  parser.add_argument('--dynamodb-table', required=True,
    help='dynamodb table')
  parser.add_argument('--receiver-email', default='xyz@example.com',
    help='receiver email address')

  options = parser.parse_args()
  AWS_REGION_NAME = options.region_name
  url_parse_result = urlparse(options.output_location, scheme='s3')
  ATHENA_QUERY_OUTPUT_BUCKET_NAME = url_parse_result.netloc
  ATHENA_WORK_GROUP_NAME = options.work_group_name
  DDB_TABLE_NAME = options.dynamodb_table

  query_string = '''SELECT dt, impressionid
FROM impressions
WHERE dt <  '2009-04-12-14-00'
  AND dt >= '2009-04-12-13-00'
ORDER BY  dt DESC LIMIT 100'''

  database = "hive_ads"
  output_location = options.output_location

  req_body = {
    "QueryString": query_string,
    "QueryExecutionContext": {
      "Database": database
    },
    "ResultConfiguration": {
      "OutputLocation": output_location
    }
  }

  if options.print_query_string:
    print(json.dumps(req_body, indent=2))
    sys.exit(0)

  event = {
    'resource': '/',
    'path': '/',
    'httpMethod': 'POST',
    'headers': {
      'Accept': '*/*',
      'CloudFront-Forwarded-Proto': 'https',
      'CloudFront-Is-Desktop-Viewer': 'true',
      'CloudFront-Is-Mobile-Viewer': 'false',
      'CloudFront-Is-SmartTV-Viewer': 'false',
      'CloudFront-Is-Tablet-Viewer': 'false',
      'CloudFront-Viewer-Country': 'KR',
      'content-type': 'application/json',
      'Host': '{restapi-id}.execute-api.{region}.amazonaws.com', #TODO
      'User-Agent': 'curl/7.64.1',
      'Via': '2.0 6f51dc97d58041fe23fd6f71e2f76dd5.cloudfront.net (CloudFront)',
      'X-Amz-Cf-Id': '7NBwUoNX6n9zqFoc7zXh-q9sL8IYS8lKFfa7efOzlRtpxfZm8TtrRw==',
      'X-Amzn-Trace-Id': 'Root=1-5fbf8fdc-3d0f1b62599773d146c56778',
      'X-Forwarded-For': '0.0.0, 0.0.0.1',
      'X-Forwarded-Port': '443',
      'X-Forwarded-Proto': 'https'
    },
    'multiValueHeaders': {
      'Accept': ['*/*'],
      'CloudFront-Forwarded-Proto': ['https'],
      'CloudFront-Is-Desktop-Viewer': ['true'],
      'CloudFront-Is-Mobile-Viewer': ['false'],
      'CloudFront-Is-SmartTV-Viewer': ['false'],
      'CloudFront-Is-Tablet-Viewer': ['false'],
      'CloudFront-Viewer-Country': ['KR'],
      'content-type': ['application/json'],
      'Host': ['{restapi-id}.execute-api.{region}.amazonaws.com'],
      'User-Agent': ['curl/7.64.1'],
      'Via': ['2.0 6f51dc97d58041fe23fd6f71e2f76dd5.cloudfront.net (CloudFront)'],
      'X-Amz-Cf-Id': ['7NBwUoNX6n9zqFoc7zXh-q9sL8IYS8lKFfa7efOzlRtpxfZm8TtrRw=='],
      'X-Amzn-Trace-Id': ['Root=1-5fbf8fdc-3d0f1b62599773d146c56778'],
      'X-Forwarded-For': ['0.0.0.0, 0.0.0.1'],
      'X-Forwarded-Port': ['443'],
      'X-Forwarded-Proto': ['https']
    },
    'queryStringParameters': {
      'user': options.receiver_email
    },
    'multiValueQueryStringParameters': {
      'user': [options.receiver_email]
    },
    'pathParameters': None,
    'stageVariables': None,
    'requestContext': {
      'resourceId': '5n6mk5fp77',
      'resourcePath': '/',
      'httpMethod': 'POST',
      'extendedRequestId': 'WnNqbGfvoAMFuwg=',
      'requestTime': '26/Nov/2020:11:22:04 +0000',
      'path': '/prod',
      'accountId': '111122223333',
      'protocol': 'HTTP/1.1',
      'stage': '{stage_name}',
      'domainPrefix': 'wystiwoho7',
      'requestTimeEpoch': 1606389724298,
      'requestId': 'bfcaab68-fcd3-4a70-9c4f-3b45ab682573',
      'identity': {
        'cognitoIdentityPoolId': None,
        'accountId': None,
        'cognitoIdentityId': None,
        'caller': None,
        'sourceIp': '54.239.119.16',
        'principalOrgId': None,
        'accessKey': None,
        'cognitoAuthenticationType': None,
        'cognitoAuthenticationProvider': None,
        'userArn': None,
        'userAgent': 'curl/7.64.1',
        'user': None
      },
      'domainName': '{restapi-id}.execute-api.{region}.amazonaws.com',
      'apiId': '{restapi-id}'
    },
    'body': json.dumps(req_body, ensure_ascii=False),
    'isBase64Encoded': False
  }

  lambda_handler(event, {})
