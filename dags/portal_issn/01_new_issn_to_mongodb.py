import logging
from datetime import datetime
import requests
from requests.exceptions import RequestException
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook


def fetch_jwt_token(session):
    logger = logging.getLogger("airflow.task")
    auth_url = "https://api.issn.org/authenticate/birembra1/issn"
    headers = {"Accept": "application/json"}

    logger.info("Solicitando Novo Token JWT")
    response = session.get(auth_url, headers=headers, timeout=10)
    response.raise_for_status()

    try:
        data = response.json()
        return data.get("token") if isinstance(data, dict) else str(data)
    except ValueError:
        return response.text.strip()


def extract_field_001(record):
    fields = record.get("fields", [])
    for field in fields:
        if isinstance(field, dict) and "001" in field:
            content = field["001"]
            if isinstance(content, dict):
                return content.get("value") or content.get("subfields")
            return content
            
    return None


def fetch_and_store_strategy(session, jwt_token, search_filters, target_collection, logger):
    url = "https://api.issn.org/search?natifjson=true"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"JWT {jwt_token}"
    }
    
    page = 0
    size = 100
    total_processed = 0

    while True:
        payload = {
            "search": search_filters,
            "page": page,
            "size": size
        }
        
        response = None
        try:
            response = session.post(url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
        except RequestException as e:
            logger.error(f"Erro ao executar a requisição POST de busca na página {page}: {e}")
            raise

        data = response.json()
        
        records = data if isinstance(data, list) else data.get("records", data.get("items", []))
        if not records:
            break

        for record in records:
            issn = extract_field_001(record)
            record["issn"] = str(issn).strip()
            target_collection.update_one(
                {"issn": issn},
                {"$set": record},
                upsert=True
            )
            total_processed += 1

        logger.info(f"Processados {len(records)} registros nesta página (Page: {page}). Total até agora: {total_processed}")
        
        if len(records) < size:
            break
            
        page += 1


def process_issn_search_strategies():
    logger = logging.getLogger("airflow.task")

    mongo_hook = MongoHook(mongo_conn_id="mongo")
    client = mongo_hook.get_conn()
    db = client["TITLE"]
    
    target_collection = db["portal_issn_novos"]
    target_collection.create_index("issn", unique=True, sparse=True)

    # Definição dos critérios de busca
    countries = "AIA,ATG,ARG,ABW,BHS,BRB,BLZ,BMU,BOL,BRA,CYM,CHL,COL,CRI,CUB,CUW,DMA,DOM,ECU,SLV,GUF,GRD,GLP,GTM,GUY,HTI,HND,JAM,MTQ,MEX,MSR,NIC,PAN,PRY,PER,PRI,KNA,LCA,MAF,VCT,SUR,TTO,TCA,URY,VEN,VGB"
    sources = "LATINDEX,OPENALEX,CROSSREF,FATCAT,SUDOC,ROAD,ZDB,EZB,DOI,DOAJ,SCOPUS,PUBMED,CABABSTRACTS,GLOBALHEALTH,SHERPA*,web of science"

    base_search_filters = [
        f"MUST=country={countries}",
        f"MUST=pubstat=c",
        f"MUST=srcname={sources}"
    ]

    strategy_1_filters = base_search_filters + ["MUST=su2=Medical+sciences"]
    strategy_2_filters = base_search_filters + ["MUST=su2=Psychology"]

    strategies = [strategy_1_filters, strategy_2_filters]

    with requests.Session() as session:
        try:
            jwt_token = fetch_jwt_token(session)

            for idx, strategy_filters in enumerate(strategies, start=1):
                logger.info(f"Iniciando coleta para a Estratégia {idx}...")
                fetch_and_store_strategy(session, jwt_token, strategy_filters, target_collection, logger)

            logger.info("Todas as estratégias foram processadas e salvas com sucesso no MongoDB.")

        except Exception as e:
            logger.error(f"Erro crítico durante o processamento das estratégias: {e}")
            raise


default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="DH_01_new_issn_to_mongodb",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mongodb", "issn", "api", "search"],
) as dag:

    process_issn_task = PythonOperator(
        task_id="process_issn_search_strategies_task",
        python_callable=process_issn_search_strategies,
    )