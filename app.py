#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import os
import aws_cdk as cdk

from aws_athena_cqrs_patterns.aws_athena_cqrs_patterns_stack import AwsAthenaCqrsPatternsStack

_env = cdk.Environment(
  account=os.environ["CDK_DEFAULT_ACCOUNT"],
  region=os.environ["CDK_DEFAULT_REGION"])

app = cdk.App()
AwsAthenaCqrsPatternsStack(app, "aws-athena-cqrs-patterns", env=_env)

app.synth()
