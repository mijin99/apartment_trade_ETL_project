from ingestion.fetch_apartment_trade import fetch_trade_data
from storage.s3_uploader import upload_raw_data
from transform.spark_session import create_spark_session
from transform.cleaning import clean_data
from transform.normalize import normalize_data
from storage.parquet_writer import write_parquet

def main():

    # 1. API 호출
    raw_json = fetch_trade_data()

    # 2. raw 저장
    raw_path = upload_raw_data(raw_json)

    # 3. spark 생성
    spark = create_spark_session()

    # 4. raw 읽기
    df = spark.read.json(raw_path)

    # 5. cleaning
    df = clean_data(df)

    # 6. normalize
    df = normalize_data(df)

    # 7. parquet 저장
    write_parquet(df)

if __name__ == "__main__":
    main()