#저장 전담

def write_parquet(df,output_path):
    (
        df.wrtie
        .mode("overwrite")
        .partitionBy("deal_year","deal_month")
        .parquet(output_path)
    )