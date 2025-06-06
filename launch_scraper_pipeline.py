import json
import boto3
import argparse

s3 = boto3.client('s3')
sns = boto3.client('sns')
eb = boto3.client('events')
iam_client = boto3.client('iam')
role_arn = iam_client.get_role(RoleName='LabRole')['Role']['Arn']

def initialize_s3(s3_bucket, n):
    '''
    Initialize S3 Bucker and update relevant files:
    one sampling_scraper.py, 
    n batch_i.json files containing page links,
    and one book_scraper.py.
    '''
    s3.create_bucket(Bucket=s3_bucket)
    # Wait until AWS confirms that bucket exists before moving on
    s3.get_waiter('bucket_exists').wait(Bucket=s3_bucket)
    s3.upload_file('book_scraper.py', s3_bucket, 'book_scraper.py')
    s3.upload_file('sampling_scraper.py', s3_bucket, 'sampling_scraper.py')
    for i in range(n):
        s3.upload_file(f'page_batch_{i + 1}.csv', s3_bucket, f'page_batch_{i + 1}.csv')

    print("S3 Bucket Initialization Complete.")

def initialize_sns(email_address):
    '''
    Initialize SNS topic and subscribe to the 
    topic with the email provided
    '''
    topic_arn = sns.create_topic(Name='MonitorEC2')['TopicArn']
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol='email',
        Endpoint=email_address
    )

    print("SNS Initialization Complete.")
    return topic_arn

def initialize_eb(topic_arn):
    '''
    Initialize one rule in EventBridge that notifies
    change in EC2 instances' states in running, pending,
    and terminated through SNS topic created by the initialize_sns(email)
    function. 
    '''
    event_pattern = json.dumps({"source": ["aws.ec2"], 
                                "detail-type": ["EC2 Instance State-change Notification"], 
                                "detail": {"state": ["terminated"]}})
    response = eb.put_rule(
        Name='scrapermonitor',
        EventPattern=event_pattern,
        State='ENABLED',
        Description='Monitor EC2 Instance State-change',
        RoleArn=role_arn,
        Tags=[
            {'Key': 'Purpose', 'Value': 'EC2Monitor'},
        ]
    )
    input_dict = {"instance-id":"$.detail.instance-id", 
                  "state":"$.detail.state", 
                  "time":"$.time", 
                  "region":"$.region", 
                  "account":"$.account"}
    eb.put_targets(Rule='scrapermonitor',
                   Targets=[{'Id': 'SNSEC2statechange',
                             'Arn': topic_arn,
                             'RoleArn': role_arn,
                             'InputTransformer': {
                                'InputPathsMap':input_dict,
                                'InputTemplate': '"At <time>, the status of your EC2 instance <instance-id> on account <account> in the AWS Region <region> has changed to <state>."'}
                            }])
    print("EventBridge Initialization Complete.")

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Launch Web Scraper Settings.")
    parser.add_argument('--s3_bucket', type=str, default='online-novel-trend',
                        help='S3 bucket name containing scraper.py and input batch files')
    parser.add_argument('--num_batches', type=int, default=8,
                        help='Number of json files to store the links.')
    parser.add_argument('--email', type=str, required=True,
                        help='Email address to receive notification when scraping job is complete')
    args = parser.parse_args()

    print("Initializing S3...")
    initialize_s3(args.s3_bucket, args.num_batches)

    print("Initializing SNS...")
    topic_arn = initialize_sns(args.email)
    initialize_eb(topic_arn)

    print('Successfully Launched!')
