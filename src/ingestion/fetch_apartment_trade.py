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
from dateutil.relativedelta import relativedelta 
import math

#api호출 
def fetch_apt_trade(lawd_cd,deal_ymd):
    api_key = config.API_SETTINGS.get("API_KEY")
    
    all_items =[] #배열?
    #인코딩키를 디코딩하여 원래 키로 되돌림.
    #unquoted_key = requests.utils.unquote(api_key)
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    
    page_no = 1
    total_pages =None
    while True: 
        params ={
            "LAWD_CD" : lawd_cd,
            "DEAL_YMD": deal_ymd,
            "serviceKey":api_key,
            "pageNo": page_no,
            "numOfRows" :1000,
        }
        print(f"api요청시작~:{url}")
        response = requests.get(url,params =params)
        print(response.status_code )
        if response.status_code !=200:
            raise Exception(f"api 호출 실패 : {response.status_code}")
        print("api요청 성공")
        data_dict = xmltodict.parse(response.text)
        body = data_dict["response"]["body"]
        if total_pages is None : 
            total_count = int(body["totalCount"])
            num_of_rows = int(body["numOfRows"])
            total_pages =math.ceil( total_count/num_of_rows)
            print (
                f"{lawd_cd}"
                f"{deal_ymd}"
                f"총{total_count}건"
                f"{total_pages}페이지"
            )
        items = body.get("items",{}).get("item",[])
        if not items :
            break
        if isinstance(items,dict):
            items=[items]
        all_items.extend(items)
        if page_no >= total_pages:
            break
        page_no +=1
        
    return data_dict

#s3에 저장 
def save_raw_to_s3(data_dict :str, lawd_cd :str, deal_ymd :str):
    if data_dict =="" :
        print("데이터 수신 실패")
        exit
    
    print ("데이터 수신 성공")
    #xml- > json변환 ( 추후 parquet변환이나 s3저장을 생각했을 때)
    convjson = json.dumps(data_dict, indent=4,ensure_ascii=False)
    
    #s3에 저장 , 폴더 구조를 이용해야되나? 일별 파티션, 증분적재 구현필
    #s3_key ="apt_trade_202604.json"
    s3_key = (
        f"apartment_trade/"
        f"deal_ymd={deal_ymd}/"
        f"lawd_cd={lawd_cd}/"
        f"apt_trade_{deal_ymd}_{lawd_cd}.json"
    )
    bucket_name =config.AWS_S3.get("AWS_RAW_BUCKET_NAME")
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
    return 0

#region목록 호출
def load_region_config():
    regions = config.API_REGION
    print(regions)
    return regions

def get_previous_month():
    deal_ymd = (
        datetime.today() - relativedelta(months=1) #한달 전 날짜 계산 
    ).strftime
    return deal_ymd

#ochestration 함수 
def collect_trade_data():
    #수행 법정동 코드 설정 
    regions = load_region_config()
    # 수행 날짜 설정
    deal_ymd = get_previous_month()
    #법정동코드 반복
    for region in regions : 
        data_dict = fetch_apt_trade(
            lawd_cd = region,
            deal_ymd = deal_ymd
        )
        save_raw_to_s3(
            data_dict = data_dict,
            region = region,
            deal_ymd = deal_ymd
        )
    
#테스트용 실행 
def fetch_api_data():
     
    #api_key = os.getenv("API_KEY")
    api_key = config.API_SETTINGS.get("API_KEY")
    bucket_name =config.AWS_S3.get("AWS_RAW_BUCKET_NAME")
    #인코딩키를 디코딩하여 원래 키로 되돌림.
    #unquoted_key = requests.utils.unquote(api_key)
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    params ={
        "LAWD_CD" :11680,
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
    collect_trade_data()
    #fetch_api_data()
 