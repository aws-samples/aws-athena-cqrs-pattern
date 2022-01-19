#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
#vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

from aws_cdk import (
  core,
  aws_apigateway as apigateway,
  aws_dynamodb as dynamodb,
  aws_ec2,
  aws_events,
  aws_events_targets,
  aws_iam,
  aws_lambda as _lambda,
  aws_logs,
  aws_s3 as s3
)

class AwsAthenaCqrsPatternsStack(core.Stack):

  def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
    super().__init__(scope, id, **kwargs)

    # The code that defines your stack goes here
    vpc_name = self.node.try_get_context("vpc_name")
    vpc = aws_ec2.Vpc.from_lookup(self, "VPC",
      is_default=True, #XXX: Whether to match the default VPC
      vpc_name=vpc_name)

    s3_bucket_name = self.node.try_get_context('s3_bucket_name')
    if s3_bucket_name:
      s3_bucket = s3.Bucket.from_bucket_name(self, 'AthenaQueryResultsBucket', s3_bucket_name)
    else:
      s3_bucket_name_suffix = self.node.try_get_context('s3_bucket_name_suffix')
      s3_bucket = s3.Bucket(self, 'AthenaQueryResultsBucket',
        bucket_name='aws-athena-cqrs-workspace-{region}-{suffix}'.format(region=core.Aws.REGION,
          suffix=s3_bucket_name_suffix))

      s3_bucket.add_lifecycle_rule(prefix='query-results/', id='query-results',
        abort_incomplete_multipart_upload_after=core.Duration.days(3),
        expiration=core.Duration.days(7))

    ddb_table = dynamodb.Table(self, "AthenaQueryStatusPerUserDDBTable",
      table_name="AthenaQueryStatusPerUser",
      partition_key=dynamodb.Attribute(name="user_id", type=dynamodb.AttributeType.STRING),
      billing_mode=dynamodb.BillingMode.PROVISIONED,
      read_capacity=15,
      write_capacity=5,
      time_to_live_attribute="expired_at"
    )

    ddb_table.add_global_secondary_index(index_name='query_id',
      partition_key=dynamodb.Attribute(name="query_id", type=dynamodb.AttributeType.STRING),
      projection_type=dynamodb.ProjectionType.KEYS_ONLY
    )

    athena_work_group = self.node.try_get_context("athena_work_group_name")

    # Query CommandHandler
    EMAIL_FROM_ADDRESS = self.node.try_get_context('email_from_address')
    query_executor_lambda_fn = _lambda.Function(self, "CommandHander",
      runtime=_lambda.Runtime.PYTHON_3_7,
      function_name="CommandHander",
      handler="command_handler.lambda_handler",
      description="athena query executor",
      code=_lambda.Code.asset("./src/main/python/CommandHander"),
      environment={
        #TODO: MUST set appropriate environment variables for your workloads.
        'AWS_REGION_NAME': core.Aws.REGION,
        'ATHENA_QUERY_OUTPUT_BUCKET_NAME': s3_bucket.bucket_name,
        'ATHENA_WORK_GROUP_NAME': athena_work_group,
        'DDB_TABLE_NAME': ddb_table.table_name,
        'EMAIL_FROM_ADDRESS': EMAIL_FROM_ADDRESS
      },
      timeout=core.Duration.minutes(5)
    )

    managed_policy = aws_iam.ManagedPolicy.from_managed_policy_arn(self,
      'AthenaFullAccessPolicy',
      'arn:aws:iam::aws:policy/AmazonAthenaFullAccess')
    query_executor_lambda_fn.role.add_managed_policy(managed_policy)

    #XXX: When I run an Athena query, I get an "Access Denied" error
    # https://aws.amazon.com/premiumsupport/knowledge-center/access-denied-athena/
    query_executor_lambda_fn.add_to_role_policy(aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      resources=[s3_bucket.bucket_arn, "{}/*".format(s3_bucket.bucket_arn)],
      actions=["s3:Get*",
        "s3:List*",
        "s3:AbortMultipartUpload",
        "s3:PutObject"
      ]))

    ddb_table_rw_policy_statement = aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      resources=[ddb_table.table_arn],
      actions=[
        "dynamodb:BatchGetItem",
        "dynamodb:Describe*",
        "dynamodb:List*",
        "dynamodb:GetItem",
        "dynamodb:Query",
        "dynamodb:Scan",
        "dynamodb:BatchWriteItem",
        "dynamodb:DeleteItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dax:Describe*",
        "dax:List*",
        "dax:GetItem",
        "dax:BatchGetItem",
        "dax:Query",
        "dax:Scan",
        "dax:BatchWriteItem",
        "dax:DeleteItem",
        "dax:PutItem",
        "dax:UpdateItem"
      ]
    )

    query_executor_lambda_fn.add_to_role_policy(ddb_table_rw_policy_statement)

    query_executor_apigw = apigateway.LambdaRestApi(self, "QueryCommanderAPI",
      handler=query_executor_lambda_fn,
      endpoint_types=[apigateway.EndpointType.EDGE],
      deploy=True,
      deploy_options=apigateway.StageOptions(stage_name="v1")
    )

    # QueryResultsHandler
    query_results_lambda_fn = _lambda.Function(self, "QueryResultsHandler",
      runtime=_lambda.Runtime.PYTHON_3_7,
      function_name="QueryResultsHandler",
      handler="query_results_handler.lambda_handler",
      description="athena query results handler",
      code=_lambda.Code.asset("./src/main/python/QueryResultsHandler"),
      environment={
        #TODO: MUST set appropriate environment variables for your workloads.
        'AWS_REGION_NAME': core.Aws.REGION,
        'DOWNLOAD_URL_TTL': '3600',
        'DDB_TABLE_NAME': ddb_table.table_name
      },
      timeout=core.Duration.minutes(5)
    )

    query_results_lambda_fn.add_to_role_policy(aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      resources=[s3_bucket.bucket_arn, "{}/*".format(s3_bucket.bucket_arn)],
      actions=["s3:Get*",
        "s3:List*",
        "s3:PutObjectAcl",
        "s3:PutObjectVersionAcl"
      ]))

    query_results_lambda_fn.add_to_role_policy(ddb_table_rw_policy_statement)

    log_group = aws_logs.LogGroup(self, "QueryResultsHandlerLogGroup",
      log_group_name="/aws/lambda/QueryResultsHandler",
      retention=aws_logs.RetentionDays.THREE_DAYS)
    log_group.grant_write(query_results_lambda_fn)

    #XXX: Athena Query State Change Event Pattern
    # {
    #   "source": [
    #     "aws.athena"
    #   ],
    #   "detail-type": [
    #     "Athena Query State Change"
    #   ],
    #   "detail": {
    #     "previousState": [
    #       "RUNNING"
    #     ],
    #     "workgroupName": [
    #       "primary"
    #     ]
    #   }
    #  }
    aws_event_pattern = aws_events.EventPattern(
      account=[core.Aws.ACCOUNT_ID],
      region=[core.Aws.REGION],
      source=['aws.athena'],
      detail_type=['Athena Query State Change'],
      detail={
        "previousState": ["RUNNING"],
        "workgroupName": [athena_work_group]
      }
    )

    lambda_fn_target = aws_events_targets.LambdaFunction(query_results_lambda_fn)
    event_rule = aws_events.Rule(self, "AthenaQueryExecutionRule",
      enabled=False,
      event_pattern=aws_event_pattern,
      description='Athena Query State Change Event',
      rule_name='AthenaQueryExecutionRule',
      targets=[lambda_fn_target]
    )
