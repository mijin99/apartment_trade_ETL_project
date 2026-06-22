#핵심 etl로직
#raw json 읽기
# flatten
# 컬럼 정제
# 타입변환
#null처리
# 날짜 컬럼 생성
#partition column생성
import traceback
import os
from src.common import config
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark
from datetime import datetime as F

def build_curated_dataset(spark,deal_ymd):
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
  
