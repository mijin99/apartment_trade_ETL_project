
#spark session생성
#s3커넥션 설정
# aws credential 연결
#parquet옵션
#timezone 
#serializer 

from pyspark.sql import Sparksession

def create_spark_session():
    spark = (
        Sparksession.builder 
        .appName("reak-estate-etl")
    
        #s3 
        .config("spark.hadoop.fs.s3a.access.key","...")
        .config("spark.hadoop.fs.s3a.secret.key",'...')
        .config("spark.hadoop.s3a.endpoint","s3-ap-northeast-2.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.S3AFileSystem")
        #parquet 
        .config("spark.sql.parquet.compression.codec","snappy")
        .getOrCreate()
    )
    return spark