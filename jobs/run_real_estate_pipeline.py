from ingestion.fetch_apartment_trade import fetch_trade_data
from transform.spark_session import create_spark_session
from transform.build_curated_dataset import build_curated_dataset

def main():

    # 1. API 호출 및 저장 
    raw_json = fetch_trade_data()


    # 2. spark 생성 및 저장 
    spark = create_spark_session()
    
    # 3. curated 로 재적재
    build_curated_dataset (spark,"202605")

    spark.stop()

if __name__ == "__main__":
    main()