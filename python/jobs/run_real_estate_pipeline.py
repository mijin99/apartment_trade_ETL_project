import sys
from pathlib import Path
# 현재 파일(run_real_estate_pipeline.py)의 위치
current_file = Path(__file__).resolve()

# 부모의 부모 폴더(python/)의 절대 경로를 동적으로 획득
project_root = current_file.parents[1]

# 파이썬 탐색 경로에 추가
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))
    
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
    run_pipeline("202506")