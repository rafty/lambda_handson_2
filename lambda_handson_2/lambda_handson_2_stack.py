from aws_cdk import core as cdk
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_s3 as s3
from aws_cdk.aws_lambda_event_sources import S3EventSource
# from aws_cdk import aws_s3_notifications as s3_notify
# from aws_cdk.aws_lambda_python import PythonFunction
# from aws_cdk import aws_lambda_python


class LambdaHandson2Stack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        # ----------------------------------------------
        # Data Lake Bucket
        # ----------------------------------------------
        raw_key: str = 'raw/sample_data.csv'

        raw_bucket = s3.Bucket(
            self,
            id='RawDataBucket',
            # bucket_name=raw_bucket_name,
            removal_policy=cdk.RemovalPolicy.DESTROY
        )

        datalake_bucket = s3.Bucket(
            self,
            id='DatalakeBucket',
            # bucket_name=raw_bucket_name,
            removal_policy=cdk.RemovalPolicy.DESTROY
        )

        # ----------------------------------------------
        # sample raw data upload function
        # ----------------------------------------------
        raw_data_function = _lambda.Function(
            self,
            'UploadFunction',
            runtime=_lambda.Runtime.PYTHON_3_8,
            timeout=cdk.Duration.seconds(30),
            code=_lambda.Code.from_asset('lambda/raw_data'),
            handler='upload_raw_data.handler',
            environment={
                'RAW_BUCKET': raw_bucket.bucket_name,
                'KEY': raw_key
            }
        )

        raw_bucket.grant_read_write(raw_data_function)

        # ----------------------------------------------
        # etl function
        # ----------------------------------------------
        awswrangler_layer = _lambda.LayerVersion(
            self,
            "LambdaAwswranglerLayer",
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_8],
            code=_lambda.AssetCode('lambda_layers/awswrangler')
        )

        etl_function = _lambda.Function(
            self,
            'EtlFunction',
            runtime=_lambda.Runtime.PYTHON_3_8,
            timeout=cdk.Duration.seconds(30),
            code=_lambda.Code.from_asset('lambda/etl'),
            handler='etl.handler',
            layers=[awswrangler_layer],
            environment={
                'DATALAKE_BUCKET': datalake_bucket.bucket_name
            }
        )

        raw_bucket.grant_read_write(etl_function)
        datalake_bucket.grant_read_write(etl_function)

        etl_function.add_event_source(
            S3EventSource(
                raw_bucket,
                events=[s3.EventType.OBJECT_CREATED_PUT],
                filters=[s3.NotificationKeyFilter(prefix='raw/', suffix='.csv')]
            )
        )
