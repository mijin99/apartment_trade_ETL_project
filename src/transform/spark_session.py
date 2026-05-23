
#spark session생성
#s3커넥션 설정
# aws credential 연결
#parquet옵션
#timezone 
#serializer 
from src.common import config
from pyspark.sql import SparkSession

def create_spark_session():
    spark = (
        SparkSession.builder 
        .appName("real-estate-etl")
    
        #s3 
        .config("spark.hadoop.fs.s3a.access.key", config.AWS_ACCESS.get("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key",config.AWS_ACCESS.get("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint",f"s3.{config.AWS_S3.get("AWS_DEFAULT_REGION")}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.S3AFileSystem")
        #parquet 
        .config("spark.sql.parquet.compression.codec","snappy")
        .getOrCreate()
    )
    return spark

def read_s3(spark):
    s3_path = "s3a://your-raw-bucket-name/your-data-folder/sample.json"
    print(f"[*] S3 데이터 읽기 시도 중... 경로: {s3_path}")
    try :
        df = spark.read.json(s3_path)
        # 데이터가 잘 읽혔는지 상위 5개 데이터와 스키마 출력
        df.show(5)
        df.printSchema()
        return df
    except Exception as e :
        print(f"읽기실패 ! {e}")
        return None
    return 0

if __name__ == "__main__":
    # 스파크 세션 생성
    spark_session = create_spark_session()
    
    # 세션을 인자로 넘겨서 실행
    read_s3(spark_session)
    
    # 작업 완료 후 세션 종료 (자원 반납)
    spark_session.stop()