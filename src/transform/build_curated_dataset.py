#핵심 etl로직
#raw json 읽기
# flatten
# 컬럼 정제
# 타입변환
#null처리
# 날짜 컬럼 생성
#partition column생성

df = spark.read.json(raw_path)
df = (
    df
    .withColumn("deal_amount",col("dealAmount").cast("int"))
    .withColumn("deal_year",year(col("dealDate")))
)