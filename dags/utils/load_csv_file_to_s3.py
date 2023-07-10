import boto3
import os

def upload_csv_file_to_s3(file_path):
    s3 = boto3.client('s3', aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                      aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))

    with open(file_path, 'rb') as file:
        try:
            s3.put_object(
                Bucket='us-accidents-bucket',
                Key='us_accidents_16_to_m23.csv',
                Body=file
            )
            print(f"Uploaded file to Amazon S3 with success")
        except Exception as e:
            print("An error occurred during the upload of the file to Amazon S3:")
            print(str(e))

