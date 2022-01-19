#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
#vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import json
import os
import logging
import pprint
from urllib.parse import urlparse

import boto3
import botocore
from boto3.dynamodb.conditions import (
  Key,
  Attr
)

LOGGER = logging.getLogger()
if len(LOGGER.handlers) > 0:
  # The Lambda environment pre-configures a handler logging to stderr.
  # If a handler is already configured, `.basicConfig` does not execute.
  # Thus we set the level directly.
  LOGGER.setLevel(logging.INFO)
else:
  logging.basicConfig(level=logging.INFO)

AWS_REGION_NAME = os.getenv('AWS_REGION_NAME', 'us-east-1')
DOWNLOAD_URL_TTL = int(os.getenv('DOWNLOAD_URL_TTL', '3600'))
DDB_TABLE_NAME = os.getenv('DDB_TABLE_NAME')
EMAIL_FROM_ADDRESS = os.getenv('EMAIL_FROM_ADDRESS')


def gen_html(elem):
  HTML_FORMAT = '''<!DOCTYPE html>
<html>
<head>
<style>
table {{
  font-family: arial, sans-serif;
  border-collapse: collapse;
  width: 100%;
}}
td, th {{
  border: 1px solid #dddddd;
  text-align: left;
  padding: 8px;
}}
tr:nth-child(even) {{
  background-color: #dddddd;
}}
</style>
</head>
<body>
<h2>Your Query Results can be downlodable</h2>
<table>
  <tr>
    <th>key</th>
    <th>value</th>
  </tr>
  <tr>
    <td>query_id</th>
    <td>{query_id}</td>
  </tr>
  <tr>
    <td>link</th>
    <td>{link}</td>
  </tr>
</table>
</body>
</html>'''

  html_doc = HTML_FORMAT.format(query_id=elem['query_id'],
    link=elem['link'])
  return html_doc


def send_email(from_addr, to_addrs, subject, html_body):
  ses_client = boto3.client('ses', region_name=AWS_REGION_NAME)
  ret = ses_client.send_email(Destination={'ToAddresses': to_addrs},
    Message={'Body': {
        'Html': {
          'Charset': 'UTF-8',
          'Data': html_body
        }
      },
      'Subject': {
        'Charset': 'UTF-8',
        'Data': subject
      }
    },
    Source=from_addr
  )
  return ret


def get_user_id_by_query_id(table, query_execution_id):
  dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION_NAME)
  ddb_table = dynamodb.Table(table)
  try:
    #TODO: should handle ProvisionedThroughputExceededException
    ddb_attributes = ddb_table.query(
      IndexName='query_id',
      KeyConditionExpression=Key('query_id').eq(query_execution_id)
    )
  except ClientError as ex:
    LOGGER.error(ex.response['Error']['Message'])
    #TODO: send alarm by sns
    raise ex
  else:
    record = {'query_id': query_execution_id}
    if 'Items' in ddb_attributes and len(ddb_attributes['Items']) == 1:
      record = dict(ddb_attributes['Items'][0])
  return record


def update_query_status(table, user_id, query_execution_id, query_state):
  dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION_NAME)
  ddb_table = dynamodb.Table(table)
  try:
    response = ddb_table.update_item(
      Key={'user_id': user_id},
      UpdateExpression='SET query_status = :query_status',
      ConditionExpression=Attr('query_id').eq(query_execution_id),
      ExpressionAttributeValues={':query_status': query_state},
      ReturnValues='UPDATED_NEW')
  except botocore.exceptions.ClientError as ex:
    if ex.response['Error']['Code'] == 'ConditionalCheckFailedException':
      LOGGER.info(ex.response['Error']['Message'])
    else:
      raise ex
  return response


def get_athena_query_result_location(query_execution_id):
  athena_client = boto3.client('athena', region_name=AWS_REGION_NAME)
  response = athena_client.get_query_execution(
    QueryExecutionId=query_execution_id
  )
  output_location = response['QueryExecution']['ResultConfiguration']['OutputLocation']
  return output_location


def create_presigned_url(bucket_name, object_name, expiration=3600):
  s3_client = boto3.client('s3', region_name=AWS_REGION_NAME)
  try:
    presigned_url = s3_client.generate_presigned_url('get_object',
                                                 Params={'Bucket': bucket_name,
                                                         'Key': object_name},
                                                 ExpiresIn=expiration)
  except botocore.exceptions.ClientError as ex:
    LOGGER.error(ex)
    return None

  return presigned_url


def lambda_handler(event, context):
  LOGGER.debug(event)

  current_query_state = event['detail']['currentState']
  if current_query_state == 'FAILED':
    raise RuntimeError('Athena Query is {}'.format(current_query_state))
  if current_query_state != 'SUCCEEDED':
    #TODO: send alert by sns
    LOGGER.info('athena query state: %s' % current_query_state)
    return

  query_execution_id = event['detail']['queryExecutionId']
  output_location = get_athena_query_result_location(query_execution_id)
  LOGGER.info(output_location)

  url_parse_result = urlparse(output_location, scheme='s3')
  bucket_name, object_name = url_parse_result.netloc, url_parse_result.path.lstrip('/')
  presigned_url = create_presigned_url(bucket_name, object_name, expiration=DOWNLOAD_URL_TTL)
  LOGGER.info('presigned_url: %s' % presigned_url)

  try:
    record = get_user_id_by_query_id(DDB_TABLE_NAME, query_execution_id)
  except Exception as ex:
    raise ex
  else:
    # send email to requester
    record['link'] = presigned_url
    user_id = record.get('user_id', EMAIL_FROM_ADDRESS)
    html = gen_html(record)
    subject = '''Athena Query Results is ready'''
    send_email(EMAIL_FROM_ADDRESS, [user_id], subject, html)
    try:
      update_query_status(DDB_TABLE_NAME, user_id, query_execution_id, current_query_state)
    except Exception as ex:
      LOGGER.error(ex)
  LOGGER.info("end")


if __name__ == '__main__':
  import argparse

  parser = argparse.ArgumentParser()
  parser.add_argument('--region-name', default='us-east-1',
    help='aws region name: default=us-east-1')
  parser.add_argument('--query-execution-id', required=True,
    help='aws athena query execution id. ex: ce8826f3-6949-4405-81e5-392745da2c95')
  parser.add_argument('--work-group-name', default='primary',
    help='aws athena work group name: default=primary')
  parser.add_argument('--dynamodb-table', required=True,
    help='aws dynamodb table')
  parser.add_argument('--sender-email', required=True,
    help='sender email address')

  options = parser.parse_args()
  AWS_REGION_NAME = options.region_name
  DDB_TABLE_NAME = options.dynamodb_table
  EMAIL_FROM_ADDRESS = options.sender_email

  event_template = {
    "account": "111122223333",
    "detail": {
      "currentState": "SUCCEEDED",
      "previousState": "RUNNING",
      "queryExecutionId": options.query_execution_id,
      "sequenceNumber": "3",
      "statementType": "DML",
      "statementType": "DML",
      "versionId": "0",
      "workgroupName": options.work_group_name
    },
    "detail-type": "Athena Query State Change",
    "id": "d9b0f8f8-1f67-6772-a390-01556bb3c09d",
    "region": options.region_name,
    "resources": [],
    "source": "aws.athena",
    "time": "2020-11-24T05:52:12Z",
    "version": "0"
  }

  for query_state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
    event = dict(event_template)
    event['detail']['currentState'] = query_state
    try:
      lambda_handler(event, {})
    except Exception:
      import traceback
      traceback.print_exc()

