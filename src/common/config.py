import yaml
from pathlib import Path

BASE_DIR =Path(__file__).resolve().parent.parent.parent

def _load_yaml(file_name):
    file_path = BASE_DIR /"configs"/file_name
    with open(file_path,"r",encoding="utf-8") as f:
        return yaml.safe_load(f)

#두개 설정을 각 최초 1회 로드      
_api_raw = _load_yaml("api_config.yaml")
_aws_raw = _load_yaml("aws_config.yaml")

#계층대로 할당
API_SETTINGS = _api_raw.get("api",{})
AWS_ACCESS = _aws_raw.get("access",{})
AWS_S3 =_aws_raw.get("s3",{})

#==========================================================
# 🔍 [테스트 영역] 이 파일만 단독 실행했을 때만 작동하는 코드입니다.
# ==========================================================
if __name__ == "__main__":
    print("\n--- config.py 단독 테스트 시작 ---")
    print(f"1. BASE_DIR (최상위폴더) 경로: {BASE_DIR}")
    print(f"2. api_config.yaml에서 읽은 원본: {_api_raw}")
    print(f"3. API_SETTINGS 결과: {API_SETTINGS}")
    print(f"4. API_KEY 가져오기 테스트: {API_SETTINGS.get('API_KEY')}")
    print("--- 테스트 끝 ---\n")