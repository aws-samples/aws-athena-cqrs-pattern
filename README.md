
# Amazon Athena CQRS(Command and Query Responsibility Segregation) Pattern
[CQRS Pattern](https://microservices.io/patterns/data/cqrs.html) Implementation with Amazon Athena
- AWS Athena query is requested through RESTful API, then the query execution results will be sent by email

## Architecture
![athena_cqrs_pattern_arch](./assets/athena_cqrs_pattern_arch.svg)

## Deployment
1. Install the CdK by referring to the [Getting Started With the AWS CDK](https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html). Create IAM User to execute cdk and register the profile into `~/.aws/config`.
The below example shows the profile information of a new IAM User, `dcdk_user` in `~/.aws/config` file.

    ```shell script
    $ cat ~/.aws/config
    [profile cdk_user]
    aws_access_key_id=AKIAIOSFODNN7EXAMPLE
    aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    region=us-east-1
    ```

1. Run `git clone` command to download the source code, and set up cdk deployment environment.

    ```shell script
    $ git clone https://github.com/aws-samples/aws-athena-cqrs-pattern.git
    $ cd aws-athena-cqrs-patterns
    $ python3 -m venv .env
    $ source .env/bin/activate
    (.env) $ pip install -r requirements.txt
    ```

2. Configure `cdk.context.json` file.

    ```json
    {
      "s3_bucket_name_suffix": "Your-S3-Bucket-Name-Suffix",
      "athena_work_group_name": "Your-Athena-Work-Group-Name",
      "email_from_address": "Your-Sender-Email-Addr"
    }
    ```
    For example,
    ```json
    {
      "s3_bucket_name_suffix": "zy2wbzt",
      "athena_work_group_name": "primary",
      "email_from_address": "sender@example.com",
    }
    ```    

   :warning: Make suure `email_from_address` is available. For more information, see [Amazon Simple Email Service - Verifying an email address identity](https://docs.aws.amazon.com/ses/latest/DeveloperGuide/verify-email-addresses.html).
   For example, you can verify the email address, `sender@amazon.com` by running the following command:
      ```shell script
      aws ses verify-email-identity --email-address sender@amazon.com
      ```

3. Deploy CDK Stack by running `cdk deploy` command
    ```shell script
    (.env) $ export CDK_DEFAULT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
    (.env) $ export CDK_DEFAULT_REGION=us-east-1
    (.env) $ cdk --profile=cdk_user deploy
    ```

4. (Optional) Clean up.
    ```shell script
    (.env) $ cdk --profile=cdk_user destroy
    ```

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

Enjoy!

# Demo
## Preparation
1. Check if there is a sample data in your region.<br/>
For example, if you are using `us-east-1`, replace `my-region` with `us-east-1` and then run the command:

    ```shell script
   $ aws s3 ls s3://{my-region}.elasticmapreduce/samples/hive-ads/tables/impressions/

      PRE dt=2009-04-12-13-00/
      PRE dt=2009-04-12-13-05/
      PRE dt=2009-04-12-13-10/
      PRE dt=2009-04-12-13-15/
      PRE dt=2009-04-12-13-20/
      PRE dt=2009-04-12-14-00/
      PRE dt=2009-04-12-14-05/
      PRE dt=2009-04-12-14-10/
      PRE dt=2009-04-12-14-15/
      PRE dt=2009-04-12-14-20/
      PRE dt=2009-04-12-15-00/
      PRE dt=2009-04-12-15-05/
    ```

2. Go to the [AWS Athena Query editor](https://console.aws.amazon.com/athena/home?#/query-editor), and then create the database, `hive_ads`.
  
    ```SQL
    CREATE DATABASE IF NOT EXISTS hive_ads;
    ```

3. Create the table in `hive_ads` database.
   
   Replace `my-region` with your region.
   For example, `s3://us-east-1.elasticmapreduce/samples/hive-ads/tables/impressions/`

    ```SQL
    CREATE EXTERNAL TABLE impressions (
      requestBeginTime string,
      adId string,
      impressionId string,
      referrer string,
      userAgent string,
      userCookie string,
      ip string,
      number string,
      processId string,
      browserCookie string,
      requestEndTime string,
      timers struct<modelLookup:string, requestTime:string>,
      threadId string,
      hostname string,
      sessionId string)
    PARTITIONED BY (dt string)
    ROW FORMAT  serde 'org.apache.hive.hcatalog.data.JsonSerDe'
      with serdeproperties ( 'paths'='requestBeginTime, adId, impressionId, referrer, userAgent, userCookie, ip' )
    LOCATION 's3://{my-region}.elasticmapreduce/samples/hive-ads/tables/impressions/';
    ```

## Send Athena Query

``` shell script
$ export API_URL=https://{restapi-id}.execute-api.{region}.amazonaws.com/{stage_name}
$ curl -X POST ${API_URL}/?user={email-address} \
  -H 'Content-Type: application/json' \
  -d'{
    "QueryString": "{query-string}",
    "QueryExecutionContext": {
      "Database": "{database}"
    },
    "ResultConfiguration": {
      "OutputLocation": "s3://bucket-name/path/to/object/"
    }
  }'
```

For example, if you wanted to perform a query on the `hive_ads.impressions` table to count the top 100 of the `impressionids` over a specific time period, you could do the following.

``` shell script
$ export API_URL=https://ewv0mp92bz.execute-api.us-east-1.amazonaws.com/v1
$ curl -X POST ${API_URL}/?user=xyz@example.com \
  -H 'Content-Type: application/json' \
  -d'{
    "QueryString": "SELECT dt, impressionid FROM impressions WHERE dt < '2009-04-12-14-00' AND dt >= '2009-04-12-13-00' ORDER BY dt DESC LIMIT 100",
    "QueryExecutionContext": {
      "Database": "hive_ads"
    },
    "ResultConfiguration": {
      "OutputLocation": "s3://aws-athena-cqrs-workspace-us-east-1-v89ca8y9vj/query-results/"
    }
  }'
```

## Query Execution Results
When the AWS Athena query is finished running, you will receive a link to download the query result file via email.

**Figure 1.** E-mail example
![athena-cqrs-pattern-email-screenshot](./assets/athena-cqrs-pattern-email-screenshot.png)

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
