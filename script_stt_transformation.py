from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# ===========================================
# =============== PARAMETER =================
# ===========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ============================================
# ================ FUNCTION ==================
# ============================================
def load_csv_file(filename, output_name, **context):
    
    # Define source path and local storage bucket path
    source_path = os.path.join(BASE_DIR, filename)
    bucket_dir  = os.path.join(BASE_DIR, "bucket")
    bucket_path = os.path.join(bucket_dir, output_name)

    os.makedirs(bucket_dir, exist_ok=True)
    print(f"Successfully make folder bucket : {bucket_path}")

    # Generate source file to local storage bucket
    df = pd.read_csv(source_path)
    df.to_csv(bucket_path, index=False)
    print(f"File {filename} already uploaded to bucket : {bucket_path}")

def cleaning_stt(input_name, output_name, **context):
    
    REQUIRED_COLUMNS = ["number", "date", "client_code", "amount", "client_type"]

    # Path for raw data and clean data in local storage bucket
    input_path  = os.path.join(BASE_DIR, "bucket", input_name)
    output_path = os.path.join(BASE_DIR, "bucket", output_name)

    df = pd.read_csv(input_path)
    
    # Process clean data
    print(f"Processing cleaning data {input_name}")
    df = df.dropna(subset=REQUIRED_COLUMNS)
    # Convert type data
    df["date"]   = pd.to_datetime(df["date"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").astype("Int64")

    # Remove full data
    df = df.dropna(subset=["date", "amount"])
    df["date"] = df["date"].dt.date

    # Save cleaned data to local storage bucket
    df.to_csv(output_path, index=False)
    print(f"{input_name} was successfully cleaned. Path: {output_path}")

def transformation_stt(**context):

    # Read stt clean data
    stt1 = pd.read_csv(os.path.join(BASE_DIR, "bucket", "stt1_clean.csv"))
    stt2 = pd.read_csv(os.path.join(BASE_DIR, "bucket", "stt2_clean.csv"))

    # Merge or union data stt1 and stt2
    merge = pd.concat([stt1, stt2], ignore_index=True)
    merge = merge.drop_duplicates("number", keep="last") # Remove duplicates value

    # Grouping amount based on client_type
    merge["Debit"]  = merge.apply(lambda r: r["amount"] if r["client_type"] == "C" else 0, axis=1)
    merge["Credit"] = merge.apply(lambda r: r["amount"] if r["client_type"] == "V" else 0, axis=1)

    transformation = (
        merge.groupby(["date", "client_code"])
        .agg(
            STT_Count=("number", "count"),
            Debit=("Debit", "sum"),
            Credit=("Credit", "sum"),
        )
        .reset_index()
    )
    print("Successfully transformation data.")

    result_path = os.path.join(BASE_DIR, "result")
    result_name = "result_transform_stt.csv"
    os.makedirs(result_path, exist_ok=True)
    print("Successfully make directory result.")

    # Save result transformation to local directory
    transformation.to_csv(os.path.join(result_path, result_name), index=False)
    print(f"Successfully create result file : {result_path}/{result_name}")

# ==================================================
# ================ DEKLARASI DAGS ==================
# ==================================================
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="transformation_stt_data",
    start_date=datetime(2025, 12, 4),
    schedule_interval=None,
    catchup=False,
):

    load_stt1 = PythonOperator(
        task_id="load_stt1",
        python_callable=load_csv_file,
        op_kwargs={"filename": "STT1.csv", "output_name": "stt1_raw.csv"}
    )

    load_stt2 = PythonOperator(
        task_id="load_stt2",
        python_callable=load_csv_file,
        op_kwargs={"filename": "STT2.csv", "output_name": "stt2_raw.csv"}
    )

    cleaning_stt1 = PythonOperator(
        task_id="cleaning_stt1",
        python_callable=cleaning_stt,
        op_kwargs={"input_name": "stt1_raw.csv", "output_name": "stt1_clean.csv"}
    )

    cleaning_stt2 = PythonOperator(
        task_id="cleaning_stt2",
        python_callable=cleaning_stt,
        op_kwargs={"input_name": "stt2_raw.csv", "output_name": "stt2_clean.csv"}
    )

    transformation_stt_task = PythonOperator(
        task_id="transformation_stt",
        python_callable=transformation_stt
    )

    [load_stt1, load_stt2] >> [cleaning_stt1, cleaning_stt2] >> transformation_stt_task

