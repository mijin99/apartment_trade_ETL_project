
# s3에 접근하기 위한 boto3. client 

client = boto3.client ('s3',
                        aws_access_key_id = AWS_ACCESS_KEY_ID,
                        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
                        region_name  = AWS_DEFAULT_REGION)
response = client.list_buckets()
bucket = s3.bucket(name =AWS_BUCKET_NAME)
print (response)


s3 = boto3.client('s3')
bucket_name = 'my-sample-bucket'
file_name = 'local_file.txt'
object_name = 'upload_file.txt'

# S3에 파일 업로드
s3.upload_file(file_name, bucket_name, object_name)
print("업로드 완료")