
#spark session생성
#s3커넥션 설정
# aws credential 연결
#parquet옵션
#timezone 
#serializer 
from src.common import config
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark
from datetime import datetime


def create_spark_session():
    spark = (
        SparkSession.builder 
        .appName("real-estate-etl")
        #config 
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.4" )
        
        #s3 
        .config("spark.hadoop.fs.s3a.access.key", config.AWS_ACCESS.get("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key",config.AWS_ACCESS.get("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint","s3.ap-southeast-2.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        #parquet 
        .config("spark.sql.parquet.compression.codec","snappy")
        .getOrCreate()
    )
    return spark

def read_s3(spark):
    # s3a://[버킷이름]/[폴더경로]/[파일명 또는 확장자]
    s3_path = "s3a://my-data-project-raw/"
    print(f"[*] S3 데이터 읽기 시도 중... 경로: {s3_path}")
    try :
        #멀티라인 옵션 json 
        df_raw = spark.read.option("multiLine","true").json(s3_path)
        #중첩구조 flatten (dot notation사용 )
        df_items = df_raw.select(col("response.body.items.item").alias("item_list"))
        #배열 데이터를 개별 행으로 쪼개기 (explode)
        df_exploded = df_items.select(explode(col("item_list")).alias("actual_item"))
        #[중간 데이터 확인] 
        #print("==중간 데이터 확인==")
        #df_exploded.printSchema()
        #[하위 필드 꺼내기]
        df_flattened = df_exploded.select("actual_item.*")
        print("==평탄화 후 데이터 확인==")
        df_flattened.printSchema()
        
        #최종 평탄회 flatten
        df_final = df_flattened.select (
            col("dealYear").cast("int").alias("deal_year"),           #계약년
            col("dealMonth").cast("int").alias("deal_month"),         #계약월
            col("dealDay").cast("int").alias("deal_day"),             # 계약일
            col("aptNm").alias("apt_name"),                           # 아파트명
            col("dealAmount").alias("deal_amount"),                   # 거래가 (만원)
            col("excluUseAr").cast("double").alias("exclusive_area"), #전용면적
            col("floor").cast("int").alias("floor"),                  # 층
            col("sggCd").alias("sgg_code"),                           #법정동 시군구코드
            col("umdNm").alias("umd_name")                            # 동이름
        )
        
        # 데이터가 잘 읽혔는지 상위 5개 데이터와 스키마 출력
        # df_final.printSchema()
        # df_final.show(5,truncate = False)
        
        # 2. 문자열 trim 처리 
        string_cols = [ "apt_name","deal_amount","sgg_code","umd_name"]
        for c in string_cols:
            df_final = df_final.withColumn(c,trim(col(c)))
            
        # 3. 빈 문자열 null값 처리 
        for c in string_cols:
            df_final = df_final.withColumn(c, when(col(c)=="",None).otherwise(col(c)))
        
        #4. 거래금액 쉼표 제거 및 숫자형으로 변환
        df_final =df_final.withColumn("deal_amount", regexp_replace(col("deal_amount"),",","").cast(int))
        
        #5. deal_date생성 년월일 yyyymmdd
        df_final = df_final.withColumn("deal_date",
                                       to_date(concat_ws("-",col("deal_year"),
                                                            lpad(col("deal_month"),2,"0"),
                                                            lpad(col("deal_day"),2,"0"))))
        #6. parquet partition 컬럼 생성
        df_final= (df_final 
                   .withColumn("year",year(col("deal_date")))
                   .withColumn("month",month(col("deal_date")))
                   )
        
        # 7.중복제거 
        df_final = df_final.dropDuplicates()
        
        #8. parquet으로 저장
        (
            df_final.write
            .mode("overview") # append 로 많이 함 
            .partitionBy("year","month")
            .parquet(config.AWS_ACCESS.get("AWS_CURATED_BUCKET_NAME"))
        )
        return df_final
    except Exception as e :
        print(f"읽기실패 ! {e}")
        return None
    return 0

if __name__ == "__main__":
    print(pyspark.__version__)

    # 스파크 테스트 
    # spark = (
    #     SparkSession.builder
    #     .master("local[*]")
    #     .appName("s3-test")
    #     .config(
    #         "spark.jars.packages",
    #         ",".join([
    #             "org.apache.hadoop:hadoop-aws:3.3.6",
    #             "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    #         ])
    #     )
    #     .getOrCreate()
    # )
    # df = spark.range(5)
    # df.show()
    
    
    # 스파크 세션 
    spark_session = create_spark_session()
    print(spark_session.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
    # 세션을 인자로 넘겨서 실행
    read_s3(spark_session)
    
    # 작업 완료 후 세션 종료 (자원 반납)
    spark_session.stop()