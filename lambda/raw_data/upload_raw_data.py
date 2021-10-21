import os
import json
import csv
from random import randint, choice
import boto3
from botocore.exceptions import ClientError
from faker import Faker

RAW_BUCKET = os.getenv('RAW_BUCKET')
KEY = os.getenv("KEY")
s3 = boto3.resource('s3')

print(f'S3 Bucket: {RAW_BUCKET}/{KEY}')


def sample_csv(temp_file_path):
    try:
        with open(temp_file_path, "w", encoding='utf-8') as f:
            writer = csv.writer(f)
            fake = Faker('ja_JP')

            # for i in range(10000):
            for i in range(10):
                name = fake.name()
                age = randint(18, 65)
                sex = choice(('M', 'F'))
                blood = choice(('A', 'B', 'O', 'AB'))
                height = randint(145, 195)
                pref = fake.prefecture()
                company = fake.company()
                job = fake.job()
                income = randint(300, 2000)
                row = [i, name, age, sex, blood, height, pref, company, job, income]
                writer.writerow(row)

        print(f'make temp file: {temp_file_path}')
        return
    except IOError as e:
        print(e)
        raise FileWriteError("ファイル作成できず・・・")


def s3_upload(temp_file_path, bucket, key):
    print(f's3_upload(): {temp_file_path}, {bucket}/{key}')
    try:
        s3_object = s3.Object(bucket, key)
        s3_object.put(Body=open(temp_file_path, "rb"))
        print(f'put data to {bucket}/{key}')
    except Exception as e:
        print(e)


def handler(event, context):
    print('request: {}'.format(json.dumps(event)))
    try:
        temp_file_path = "/tmp/sample.csv"
        sample_csv(temp_file_path)
        s3_upload(temp_file_path, RAW_BUCKET, KEY)
        return {"bucket": RAW_BUCKET, "key": KEY}

    except FileWriteError:
        return {"bucket": None, "key": None}
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "NoSuchBucket":
            return {"bucket": None, "key": None}
    except Exception as e:
        print(e)


class FileWriteError(IOError):
    """file create error"""
    pass
