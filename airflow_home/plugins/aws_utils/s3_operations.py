import awswrangler as wr
import configparser
import boto3


def check_output_quality():
    # Config parser
    config = configparser.ConfigParser()
    config.read('airflow_home/dl.cfg')

    dl_output_data = config['S3']['OUTPUT_BUCKET']

    output_full_path = dl_output_data + '/songs'

    # We read a few records of the written parquet files to check the presence of some columns
    parquet_files = wr.s3.list_objects(output_full_path)

    not_temporary_parquet_files = filter(lambda x: 'temporary' not in x, parquet_files)

    filter_result = [x for x in not_temporary_parquet_files]

    # Check if any records were inserted to the s3 table
    if len(filter_result) == 0:
        raise ValueError("No rows persisted for the given table")

    # Read the first valid path
    df = wr.s3.read_parquet(path=filter_result[0],
                            chunked=10, dataset=True, use_threads=True)
    for row in df:
        if 'song_id' not in row.columns:
            raise ValueError("Failed to detect song_id column")

        if 'title' not in row.columns:
            raise ValueError("Failed to detect title column")


def upload_code():
    # Config parser
    config = configparser.ConfigParser()
    config.read('airflow_home/dl.cfg')
    etl_file = config['SPARK']['FILE_PATH']

    s3_client = boto3.client(
        's3',
        region_name='us-west-2',
        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'],
    )
    etl_bucket = config['S3']['CODE_BUCKET']
    s3_client.upload_file(etl_file, etl_bucket, 'etl.py')


def create_s3_buckets():
    # Config parser
    config = configparser.ConfigParser()
    config.read('airflow_home/dl.cfg')
    # S3 settings
    s3_client = boto3.client(
        's3',
        region_name='us-west-2',
        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'],
    )
    s3_resource = boto3.resource('s3')

    etl_bucket = config['S3']['CODE_BUCKET']
    etl_output_bucket = config['S3']['OUTPUT_BUCKET']
    emr_log_bucket = config['S3']['LOG_BUCKET']

    create_bucket(s3_client, s3_resource, etl_output_bucket)
    create_bucket(s3_client, s3_resource, etl_bucket)
    create_bucket(s3_client, s3_resource, emr_log_bucket)


def create_bucket(s3_client, s3_resource, bucket_name):
    location = {'LocationConstraint': 'us-west-2'}
    if not s3_resource.Bucket(bucket_name).creation_date:
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)


