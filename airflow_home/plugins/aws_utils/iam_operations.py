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