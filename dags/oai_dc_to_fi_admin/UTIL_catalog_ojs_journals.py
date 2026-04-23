import logging
import re
import requests
from datetime import datetime
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator


def list_ojs_journals():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('current', 'TITLE')    
    
    query = {
        "editor_cc_code": {"$ne": None},
        "online": {"$ne": None}
    }
    projection = {"online": 1, "id": 1, "_id": 0}
    results = collection.find(query, projection)
    
    journals = list(results)
    logger.info(f"{len(journals)} periódicos encontrados.")
    
    return [[journals[i:i + 25]] for i in range(0, len(journals), 25)]


def harvest_oai_url(journals):
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')

    for journal in journals:
        journal_id = journal.get('id')
        logger.info(f"Processando periódico {journal_id}.")
    
        online_entries = journal.get('online', [])
        
        if not isinstance(online_entries, list):
            continue

        valid_urls = []
        for entry in online_entries:
            # Extrai URL do subcampo ^b
            match = re.search(r'\^b(http[^\^]+)', entry)
            if match:
                url = match.group(1)
                # Remove URLs que nao sao OJS                
                excluded_patterns = [
                    ".elsevier.", 
                    "scielo.", 
                    ".sciencedirect.", 
                    ".pubmed", 
                    ".nih.gov",
                    ".springer.",
                    ".wiley.",
                    ".annualreviews.",
                    ".sagepub."
                ]
                if not any(pattern in url.lower() for pattern in excluded_patterns):
                    valid_urls.append(url)

        
        if valid_urls:
            for url in valid_urls:
                oai_url = None
                if "/index.php/" in url:
                    oai_url = url.split("/index.php/")[0] + "/index.php/index/oai"
                else:
                    # Fallback para o root /
                    oai_url = url.rstrip('/') + "/oai"

                if oai_url:                    
                    try:
                        # Segue o redirect caso exista
                        head_response = requests.head(oai_url, allow_redirects=True, timeout=10)
                        final_url = head_response.url
                        
                        if not final_url.endswith('/oai'):
                            final_url = final_url.split('?')[0]
                            final_url = final_url.rstrip('/') + "/oai"
                        
                        # Testa se o endpoint OAI-PMH é válido
                        response = requests.get(final_url, params={'verb': 'Identify'}, timeout=10)
                        if response.status_code == 200 and '<OAI-PMH' in response.text:
                            logger.info(f"Confirmed OAI-PMH endpoint for {journal_id}: {final_url}")

                            mongo_hook.update_many(
                                'ojs_oai_sources',
                                {'journal_id': journal_id},
                                {'$set': {
                                    'journal_id': journal_id,
                                    'oai_url': final_url,
                                    'updated_at': datetime.now()
                                }},
                                upsert=True,
                                mongo_db='TITLE'
                            )

                        else:
                            logger.warning(f"URL {final_url} did not return a valid OAI-PMH response.")
                    except Exception as e:
                        logger.error(f"Error validating OAI-PMH endpoint for {journal_id}: {str(e)}")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 15),
    'retries': 0
}
with DAG(
    'DH_UTIL_catalog_ojs_journals',
    default_args=default_args,
    description='Data Harvesting - Catalogação de periódicos OJS para o MongoDB',
    tags=["data_harvesting", "mongodb", "ojs", "journals"],
    schedule=None,
    catchup=False
) as dag:
    list_journals_task = PythonOperator(
        task_id='list_ojs_journals',
        python_callable=list_ojs_journals
    )
    harvest_oai_url_task = PythonOperator.partial(
        task_id='harvest_oai_url',
        python_callable=harvest_oai_url
    ).expand(op_args=list_journals_task.output)