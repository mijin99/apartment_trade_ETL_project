
#spark session생성
#s3커넥션 설정
# aws credential 연결
#parquet옵션
#timezone 
#serializer 
import traceback
import os
from src.common import config
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark
from datetime import datetime as F


def create_spark_session():
    spark = (
        SparkSession.builder 
        .appName("real-estate-etl")
        #config 
        #.config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.4" )
        # bundle 라이브러리 쌍 
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        #s3 
        .config("spark.hadoop.fs.s3a.access.key", config.AWS_ACCESS.get("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key",config.AWS_ACCESS.get("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
       
        #글로벌 엔드포인트 지정 
        .config("spark.hadoop.fs.s3a.endpoint","s3.ap-southeast-2.amazonaws.com")
        #교차 리전 접근 허용 sdk가 올바른 이전으로 요청 라우팅 
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.experimental.aws.s3.labels.cross-region", "true")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.hadoop.io.native.lib.available", "false")
         #parquet 
        .config("spark.sql.parquet.compression.codec","snappy")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark

def read_s3(spark,deal_ymd):
    # s3a://[버킷이름]/[폴더경로]/[파일명 또는 확장자]
    raw_path = config.AWS_S3.get("AWS_RAW_BUCKET_NAME")
    s3_path = (
        f"s3a://{raw_path}/"
        f"apartment_trade/"
        f"deal_ymd={deal_ymd}/"
    )
    #"s3a://my-data-project-raw/"
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
            col("floor").cast("int").alias("floor_no"),                  # 층
            col("sggCd").alias("sgg_code"),                           #법정동 시군구코드
            col("umdNm").alias("umd_name")                            # 동이름
        )
        #[디버깅]변환 확인 
        #df_final.select("deal_year", "deal_month").show(5)
        
        # 데이터가 잘 읽혔는지 상위 5개 데이터와 스키마 출력
        # df_final.printSchema()
        # df_final.show(5,truncate = False)
        
        # 2. 문자열 trim 처리 
        string_cols = [ "apt_name","deal_amount","sgg_code","umd_name"]
        for c in string_cols:
            df_final = df_final.withColumn(c,trim(col(c)))
            
        # 3. 빈 문자열 null값 처리 
        # for c in string_cols:
        #     df_final = df_final.withColumn(c, when(col(c)=="",None).otherwise(col(c)))
        df_final = df_final.select([
            when(col(c) == "", None).otherwise(col(c)).alias(c) if c in string_cols else col(c)
            for c in df_final.columns
        ])
        #4. 거래금액 쉼표 제거 및 숫자형으로 변환
        df_final =df_final.withColumn("deal_amount", regexp_replace(col("deal_amount"),",","").cast("int"))
        
        #5. deal_date생성 년월일 yyyymmdd
        df_final = ( df_final
                        .withColumn("deal_date",
                                    make_date(
                                        col("deal_year"),
                                        col("deal_month"),
                                        col("deal_day")
                                    )
                        )
                        .withColumn("year",year(col("deal_date")))
                        .withColumn("month",month(col("deal_date")))
        )
        
        #삭제
        #df_final.select("deal_year", "deal_month", "deal_date").show(5)
        #6. parquet partition 컬럼 생성
        df_final= (df_final 
                   .withColumn("year",year(col("deal_date")))
                   .withColumn("month",month(col("deal_date")))
                   )
        
        # 7.중복제거 
        df_final = df_final.dropDuplicates()
        df_final = df_final.drop(
                "deal_year",
                "deal_month",
                "deal_day"
        )
        
        save_s3_name = config.AWS_S3.get("AWS_CURATED_BUCKET_NAME")
        s3_save_path = f"s3a://{save_s3_name}/"
        
        # print("save path:", repr(s3_save_path))
        # print("length:", len(s3_save_path))
        #저장 테스트 
        #df_final.limit(1).write.mode("append").partitionBy("year","month").parquet(s3_save_path)
        #8. parquet으로 저장
        
        #[최종스키마 구조 확인] - deal year,month 리팩토링때 제거!!!!!!1
        df_final.printSchema()
        (
            df_final.write
            .mode("overwrite") # append 로 많이 함 
            .partitionBy("year","month")
            .format("parquet")
            .save(s3_save_path)
           # .parquet(s3_save_path)
        )
        
        return df_final
    except Exception as e :
        print(f"읽기실패 ! {e}")
        traceback.print_exc()
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
    
    
    os.environ["HADOOP_HOME"] = r"C:\Users\j\OneDrive\Desktop\김미진\260517_ETL프로젝트\spark\hadoop"
    os.environ["PATH"] += ";" + r"C:\Users\j\OneDrive\Desktop\김미진\260517_ETL프로젝트\spark\hadoop\bin"
    print(os.environ.get("HADOOP_HOME"))
    
    #spark 생성 직후 찎어보기!!!!!!!!!!!!!!!!!!!!!!!1
    #spark.range(1).write.mode("overwrite").csv("tmp_test")
    #에러나면 하둡 로컬 파일시스템 문제 
    #.config("spark.hadoop.io.native.lib.available", "false")
    #spark.sparkContext._jsc.hadoopConfiguration().set(
    # "io.native.lib.available",
    # "false"
    # )
    # 스파크 세션 
    spark_session = create_spark_session()
    #print("Spark :", spark.version)
    #spark_session.range(1).write.mode("overwrite").parquet("tmp_parquet")
    #하둡 버전 찍기
    #print(spark_session.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
    # 세션을 인자로 넘겨서 실행
    read_s3(spark_session,"202605")
    
    # 작업 완료 후 세션 종료 (자원 반납)
    spark_session.stop()