import boto3
import argparse

def list_s3_files(bucket, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    files = []
    for page in page_iterator:
        contents = page.get('Contents', [])
        files.extend(obj['Key'] for obj in contents)
    return files

def split_batches(file_list, num_batches):
    total = len(file_list)
    n = total // num_batches
    rem = total % num_batches
    split_list = []
    start = 0

    for i in range(num_batches):
        end = start + n + (1 if i < rem else 0)
        split_list.append(file_list[start:end])
        start = end

    return split_list

def launch_scraper_instances(s3_bucket, job, ami_id, instance_type, key_name,
                             iam_instance_profile, num_batches, year):
    ec2 = boto3.client("ec2", "us-east-1")
    # Launch `num_batches` EC2 instances

    if job == 'page':
        for i in range(1, num_batches + 1):
            batch_filename = f"page_batch_{i}.csv"
            print(f"Launching EC2 instance for {batch_filename}...")
            user_data_script = f"""#!/bin/bash
                                    set -e
                                    python3 -m venv env
                                    source env/bin/activate
                                    pip install requests beautifulsoup4 boto3

                                    aws s3 cp s3://{s3_bucket}/sampling_scraper.py /home/ec2-user/sampling_scraper.py
                                    aws s3 cp s3://{s3_bucket}/{batch_filename} /home/ec2-user/{batch_filename}

                                    python3 /home/ec2-user/sampling_scraper.py \
                                        --input_file /home/ec2-user/{batch_filename} 

                                    shutdown -h now
                                """
            ec2.run_instances(
                ImageId=ami_id,
                InstanceType=instance_type,
                KeyName=key_name,
                MinCount=1,
                MaxCount=1,
                UserData=user_data_script,
                TagSpecifications=[{
                    'ResourceType': 'instance',
                    'Tags': [{'Key': 'Purpose', 'Value': 'ScraperWorker-Page'},
                            {'Key': 'BatchFile', 'Value': batch_filename}]
                }],
                InstanceInitiatedShutdownBehavior='terminate',
                IamInstanceProfile={
                    'Name': iam_instance_profile
                }
            )
    elif job == 'book':
        all_files = list_s3_files(args.s3_bucket, prefix=f'book_batch_{year}')
        batches = split_batches(all_files, args.num_batches)
        for i, batch_files in enumerate(batches):
            files = ' '.join(batch_files)
            print(f"Launching EC2 instance for {files}...")
            commands = ""
            for file in batch_files:
                commands += f"""
                            aws s3 cp s3://{s3_bucket}/{file} /home/ec2-user/{file}
                            python3 /home/ec2-user/book_scraper.py --input_file /home/ec2-user/{file}
                            """
            user_data_script = f"""#!/bin/bash
                                    set -e
                                    python3 -m venv env
                                    source env/bin/activate
                                    pip install requests beautifulsoup4 boto3

                                    aws s3 cp s3://{s3_bucket}/book_scraper.py /home/ec2-user/book_scraper.py

                                    {commands}

                                    shutdown -h now
                                """
            ec2.run_instances(
                ImageId=ami_id,
                InstanceType=instance_type,
                KeyName=key_name,
                MinCount=1,
                MaxCount=1,
                UserData=user_data_script,
                TagSpecifications=[{
                    'ResourceType': 'instance',
                    'Tags': [{'Key': 'Purpose', 'Value': 'ScraperWorker-Book'},
                            {'Key': 'BatchFile', 'Value': f'{year}_{i}'}]
                }],
                InstanceInitiatedShutdownBehavior='terminate',
                IamInstanceProfile={
                    'Name': iam_instance_profile
                }
            )
    else:
        return "Please select valid job type."

    print(f"\nLaunched {num_batches} EC2 instances for {job} scraping.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Launch EC2 instances for parallel scraping.")
    parser.add_argument('--s3_bucket', type=str, default='online-novel-trend',
                        help='S3 bucket name containing relevant files')
    parser.add_argument('--job', type=str, required=True, help='Job Type: Page Scraping(page) or Book Scraping(book)')
    parser.add_argument('--ami_id', type=str, default='ami-00a929b66ed6e0de6',
                        help='AMI ID to use for launching EC2 instances (default: ami-00a929b66ed6e0de6 - Amazon Linux 2023 AMI 2023.7.20250331.0 x86_64 HVM kernel-6.1 ).')
    parser.add_argument('--instance_type', type=str, default='t2.micro',
                        help='EC2 instance type (default: t2.micro).')
    parser.add_argument('--key_name', type=str, default='vockey',
                        help='Name of your EC2 key pair (default: vockey).')
    parser.add_argument('--iam_instance_profile', type=str, default='LabInstanceProfile',
                        help='IAM Instance Profile to allow EC2 instance access to private S3 bucket and other AWS Services (default: LabInstanceProfile).')
    parser.add_argument('--num_batches', type=int, default=8,
                        help='Number of EC2 instances to launch for processing the same number of input batch files (default: 8).')
    parser.add_argument('--year', type=int, default=2024,
                        help='Year')
    args = parser.parse_args()

    launch_scraper_instances(args.s3_bucket, args.job, args.ami_id, args.instance_type,
                             args.key_name, args.iam_instance_profile,
                             args.num_batches, args.year)