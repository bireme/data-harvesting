import logging
from datetime import datetime
import requests
from requests.exceptions import RequestException
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook


def fetch_jwt_token():
    logger = logging.getLogger("airflow.task")
    auth_url = "https://api.issn.org/authenticate/birembra1/issn"
    headers = {"Accept": "application/json"}

    logger.info("Solicitando Novo Token JWT")
    response = requests.get(auth_url, headers=headers, timeout=10)
    response.raise_for_status()

    try:
        data = response.json()
        return data.get("token") if isinstance(data, dict) else str(data)
    except ValueError:
        return response.text.strip()


def process_issn_records():
    logger = logging.getLogger("airflow.task")

    mongo_hook = MongoHook(mongo_conn_id="mongo")
    client = mongo_hook.get_conn()
    db = client["TITLE"]
    
    source_collection = db["current"]
    target_collection = db["portal_issn"]
    target_collection.create_index("issn", unique=True)

    query = {
        "status": "C",
        "issn": {
            "$exists": True,
            "$nin": [None, "", " "]
        }
    }
    cursor = source_collection.find(query)

    jwt_token = None
    for index, record in enumerate(cursor):
        if index % 1000 == 0:
            try:
                jwt_token = fetch_jwt_token()
            except RequestException as e:
                logger.error(f"Failed to fetch JWT token at index {index}: {e}")
                raise

        issn = record.get("issn")
        if not issn or not str(issn).strip():
            continue

        issn_clean = str(issn).strip()
        notice_url = f"https://api.issn.org/notice/{issn_clean}?natifjson=true"
        headers = {
            "Accept": "application/json",
            "Authorization": f"JWT {jwt_token}"
        }

        try:
            response = requests.get(notice_url, headers=headers, timeout=10)
            response.raise_for_status()
            response_json = response.json()
            logger.info(f"Processed ISSN: {issn_clean}")

            response_json['issn'] = issn_clean
            target_collection.update_one(
                {"issn": issn_clean},
                {"$set": response_json},
                upsert=True
            )
        except RequestException as e:
            logger.error(f"Failed request for ISSN {issn_clean}: {e}")

    logger.info("Finished processing all matching records.")


default_args = {
    "owner": "airflow",
    "retries": 0,
}
with DAG(
    dag_id="DH_01_issn_to_mongodb",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mongodb", "issn", "api"],
) as dag:

    process_issn_task = PythonOperator(
        task_id="process_issn_records_task",
        python_callable=process_issn_records,
    )