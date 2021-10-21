import os
import json
import urllib.parse
import boto3
import botocore
import awswrangler as wr
import pandas as pd

DATALAKE_BUCKET = os.getenv('DATALAKE_BUCKET')
s3_client = boto3.client('s3')


def download_csv_data_to_tmp(bucket, key, filename):

    try:
        file_path = os.path.join('/tmp', os.path.basename(filename))
        s3_client.download_file(bucket, key, file_path)
        return file_path
    except botocore.exceptions.ClientError as e:
        print(e)
        raise e


def handler(event, context):
    print('request: {}'.format(json.dumps(event)))

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    # temp_file_path = "/tmp/raw_data.csv"
    file_name = 'down_load_data.csv'
    output_path = 'datalake/samples'

    try:
        # s3_object = s3.Object(bucket, key)
        # body = s3_object.get()['Body'].read().decode('utf-8')
        # print(f'body:\n{body}')

        data_file_path = download_csv_data_to_tmp(bucket, key, file_name)
        dataframe = pd.read_csv(data_file_path, encoding='utf-8')
        # dataframe.to_parquet()
        wr.s3.to_parquet(
            df=dataframe,
            # path=f's3://{bucket}/{output_path}/sample.parquet'
            path=f's3://{DATALAKE_BUCKET}/sample.parquet'
        )

        return {}

    except Exception as e:
        print(e)
        raise e
