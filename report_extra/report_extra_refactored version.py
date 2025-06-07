import os
import json
import time
import traceback
import csv

import boto3
import pika
from boto3.s3.transfer import TransferConfig
from tqdm import tqdm


class Config:
    def __init__(self):
        self.AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
        self.AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
        self.ARCHIVE_BUCKET = os.getenv('ARCHIVE_BUCKET')
        self.BUCKET = os.getenv('BUCKET')
        self.PROJECT = os.getenv('PROJECT')
        self.EXCHANGE = os.getenv('RABBIT_EXCHANGE')
        self.IDS_KEY = os.getenv('IDS_KEY')
        self.FINISHED_LATEST_KEY = os.getenv('FINISHED_LATEST_KEY')
        self.TEMP_REPORT_ZIP_FILENAME = 'temp_report.csv.zip'
        self.TEMP_REPORT_CSV_FILENAME = 'temp_report.csv'
        self.NUM_TRACKS = int(os.getenv('NUM_TRACKS', '0'))
        self.RABBIT_HOST = os.getenv('RABBIT_HOST')
        self.RABBIT_USER = os.getenv('RABBIT_USER')
        self.RABBIT_PWD = os.getenv('RABBIT_PWD')
        self.DEFAULT_THINNING = int(os.getenv("DEFAULT_THINNING", 10))
        self.USE_CONTENT_RECOGNITION = os.getenv('USE_CONTENT_RECOGNITION', 'false').lower() == 'true'
        self.CONTENT_RECOGNITION_URL = "https://content-recognition-api.orfium.com/api/v1/search/"
        self.CONTENT_RECOGNITION_SECRET = os.getenv("CONTENT_RECOGNITION_SECRET", '')
        self.FINGERPRINTS_QUARTER = os.getenv("FINGERPRINTS_QUARTER")
        self.DOWNLOAD_BUCKET = os.getenv("DOWNLOAD_BUCKET")

        # Validate critical configs
        assert self.AWS_ACCESS_KEY, "AWS_ACCESS_KEY is not set"
        assert self.AWS_SECRET_KEY, "AWS_SECRET_KEY is not set"
        assert self.ARCHIVE_BUCKET, "ARCHIVE_BUCKET is not set"
        assert self.RABBIT_HOST, "RABBIT_HOST is not set"

        self.session = boto3.Session(
            aws_access_key_id=self.AWS_ACCESS_KEY,
            aws_secret_access_key=self.AWS_SECRET_KEY,
        )
        self.s3 = self.session.resource('s3')

        self.rabbit_credentials = pika.PlainCredentials(
            username=self.RABBIT_USER, password=self.RABBIT_PWD
        )


class BaseCSVDialect(csv.Dialect):
    delimiter = ','
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    lineterminator = '\n'
    quoting = csv.QUOTE_MINIMAL


SoundcloudReportDialect = BaseCSVDialect
YoutubeSourceReportDialect = BaseCSVDialect
JasracDialect = BaseCSVDialect


def get_values(body: str):
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON format")

    if "key" not in data:
        raise KeyError("Missing 'key' in request")

    return data["key"]


def download(key: str, config: Config, bucket: str = None, filename: str = None):
    bucket = bucket or config.ARCHIVE_BUCKET
    filename = filename or config.TEMP_REPORT_CSV_FILENAME

    try:
        ext = key.split(".")[-1]
        if ext == "zip":
            config.s3.Bucket(bucket).download_file(key, config.TEMP_REPORT_ZIP_FILENAME)
        else:
            transfer_config = TransferConfig(
                multipart_threshold=1024 * 1024,
                max_concurrency=10,
                multipart_chunksize=1024 * 1024,
                use_threads=True
            )
            config.s3.Object(bucket, key).download_file(filename, Config=transfer_config)
    except Exception as e:
        traceback.print_exc()
        raise e

    return os.path.join(os.getcwd(), filename)


# Example usage
if __name__ == "__main__":
    cfg = Config()
    print("Config loaded successfully.")
    # Example: key = get_values('{"key": "somefile.csv"}')
    # file_path = download(key, cfg)
