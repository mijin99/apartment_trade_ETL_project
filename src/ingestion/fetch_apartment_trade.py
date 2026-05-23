import os 
import sys
from src.common import config
from dotenv import load_dotenv
import requests
import json 
from datetime import datetime
import pandas as pd
import xmltodict
import logging
import boto3
import sys


def show_data():
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket='my-data-project-raw',prefix ='')
    
def fetch_api_data():
    #api_key = os.getenv("API_KEY")
    api_key = config.API_SETTINGS.get("API_KEY")
    bucket_name ="my-data-project-raw"
    #인코딩키를 디코딩하여 원래 키로 되돌림.
    #unquoted_key = requests.utils.unquote(api_key)
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    params ={
        "LAWD_CD" :11740,
        "DEAL_YMD":202604,
        "serviceKey":api_key,
        "pageNo": 1,
        "numOfRows" :10,
    }
    print(f"api요청시작~:{url}")
    response = requests.get(url,params =params)
    print(response.status_code )
    
    if response.status_code ==200 :
        print ("데이터 수신 성공")
        #xml- > json변환 ( 추후 parquet변환이나 s3저장을 생각했을 때)
        data_dict = xmltodict.parse(response.text)
        convjson = json.dumps(data_dict, indent=4,ensure_ascii=False)
        
        #s3에 저장 , 폴더 구조를 이용해야되나? 일별 파티션, 증분적재 구현필
        s3_key ="apt_trade_202604.json"
        try:
            s3_client = boto3.client('s3')
            s3_client.put_object(
                Bucket =bucket_name,
                Key =s3_key,
                Body = convjson.encode('utf-8'),
                ContentType='application/json'
            )
            print(f"[완료] s3버킷 '{bucket_name}'에 '{s3_key}' 파일로 저장완료")
        except Exception as e:
            print (f"[오류]s3업로드중 오류 {e}")
    else:
        print(f"공공데이터 요청 실패 {response.status_code}")

        
    #

if __name__ =="__main__":
    print(sys.executable)
    print(f"파일 실행위치:  {sys.path}")
    print(f"config위치 {config.__file__}")
    fetch_api_data()
   # show_data() 