import boto3
import configparser
import json


def create_iam_role():
    # Config parser
    config = configparser.ConfigParser()
    config.read('airflow_home/dl.cfg')

    iam_client = boto3.client('iam')
    iam_resource = boto3.resource('iam')
    iam_role_name = config.get('IAM', 'ROLE_NAME', fallback='MyEmrRole')
    role = iam_resource.Role(iam_role_name)

    if role.name:
        return 'Role already exists'
    else:
        role = iam_client.create_role(
            RoleName=iam_role_name,
            Description='Allows EMR to call AWS services on your behalf',
            AssumeRolePolicyDocument=json.dumps({
                'Version': '2012-10-17',
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'elasticmapreduce.amazonaws.com'}
                }]
            })
        )

    iam_client.attach_role_policy(
        RoleName=iam_role_name,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess'
    )

    iam_client.attach_role_policy(
        RoleName=iam_role_name,
        PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
    )

    return 'Role ' + iam_role_name + 'created'


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


def create_emr_cluster():
    # Config parser
    config = configparser.ConfigParser()
    config.read('airflow_home/dl.cfg')

    # Buckets Configuration
    emr_log_bucket = config['S3']['LOG_BUCKET']
    etl_bucket = config['S3']['CODE_BUCKET']

    # Datalake Configuration
    dl_input_data = config['DATALAKE']['INPUT_DATA']
    dl_output_data = config['DATALAKE']['OUTPUT_DATA']

    etl_file = config['SPARK']['FILE_PATH']

    # Emr Client
    emr_client = boto3.client(
        'emr',
        region_name='us-west-2',
        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY']
    )

    cluster_id = emr_client.run_job_flow(
        Name='spark-emr-cluster',
        ReleaseLabel='emr-5.28.0',
        LogUri='s3://' + emr_log_bucket + '-us-west-2',
        Applications=[
            {
                'Name': 'Spark'
            },

        ],
        Configurations=[
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3"
                        }
                    }
                ]
            }
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 3,
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
        },
        Steps=[
            {
                'Name': 'Setup Debugging',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['state-pusher-script']
                }
            },
            {
                'Name': 'Setup - copy files',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 's3', 'cp', 's3://' + etl_bucket, '/home/hadoop/',
                             '--recursive']
                }
            },
            {
                'Name': 'Run Spark',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', '/home/hadoop/' + 'etl.py',
                             dl_input_data, dl_output_data]
                }
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='MyEmrRole'
    )

    return 'cluster created with the step...' + cluster_id['JobFlowId']


def create_bucket(s3_client, s3_resource, bucket_name):
    location = {'LocationConstraint': 'us-west-2'}
    if not s3_resource.Bucket(bucket_name).creation_date:
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)


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


def terminate_stale_clusters():
    # Config parser
    config = configparser.ConfigParser()
    config.read('airflow_home/dl.cfg')

    # Emr Client
    emr_client = boto3.client(
        'emr',
        region_name='us-west-2',
        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY']
    )

    response = emr_client.list_clusters(ClusterStates=[
        'WAITING',
    ])

    clusters_to_terminate = []

    if response['Clusters'] and len(response['Clusters']) > 0:
        for cluster in response['Clusters']:
            clusters_to_terminate.append(cluster['Id'])

    if len(clusters_to_terminate) > 0:
        emr_client.terminate_job_flows(JobFlowIds=clusters_to_terminate)
        return 'Clusters in WAITING state terminated successfully'

    return 'No WAITING clusters found.'
