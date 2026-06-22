from src.ingestion.fetch_apartment_trade import collect_trade_data
from src.transform.spark_session import create_spark_session
from src.transform.build_curated_dataset import build_curated_dataset

def run_pipeline(deal_ymd):

    # 1. API 호출 및 저장 
    collect_trade_data()


    # 2. spark 생성 및 저장 
    spark = create_spark_session()
    
    # 3. curated 로 재적재
    build_curated_dataset (spark,deal_ymd)

    spark.stop()

if __name__ == "__main__":
    run_pipeline()