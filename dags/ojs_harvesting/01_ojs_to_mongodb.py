import logging
import re
import cloudscraper
import xmltodict
from datetime import datetime
import time
from datetime import timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from pymongo import ReplaceOne


def list_ojs_journals(**context):
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('ojs_oai_sources', 'TITLE')
    
    query = {"in_title": True}
    journal_id = context.get("params", {}).get("journal_id")
    if journal_id:
        logger.info(f"Journal_id definido {journal_id}.")
        query["journal_id"] = journal_id

    projection = {"journal_id": 1, "oai_url": 1, "_id": 0}
    results = list(collection.find(query, projection))
    
    logger.info(f"{len(results)} periódicos encontrados.")
    
    return [[results[i:i + 5]] for i in range(0, len(results), 5)]


def harvest_oai_url(journals, **context):
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    mongo_db = 'OJS'
    
    scraper = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'desktop': True
        }
    )

    from_date = context.get("params", {}).get("from_date")
    if from_date:
        logger.info(f"from_date definido {from_date}.")

    metadata_prefixes = ['oai_dc', 'marcxml']

    for journal in journals:
        journal_id = journal.get('journal_id')
        oai_url = journal.get('oai_url')

        for prefix in metadata_prefixes:
            logger.info(f"Iniciando {journal_id} com prefixo {prefix}.")
            collection_name = f"{journal_id}_{prefix}"
            coll = mongo_hook.get_collection(collection_name, mongo_db=mongo_db)
            coll.create_index("id", unique=True)
            
            params = {
                'verb': 'ListRecords',
                'metadataPrefix': prefix
            }
            if from_date:
                params['from'] = from_date

            while params:
                response_text = None
                max_retries = 3
                
                for attempt in range(max_retries):
                    try:
                        response = scraper.get(oai_url, params=params, timeout=60)
                        
                        if "awsWafCookieDomainList" in response.text:
                            logger.error(f"Bloqueio WAF detectado para {oai_url}. Scraper não conseguiu resolver.")
                            return 

                        response.raise_for_status()
                        response_text = response.text
                        break
                    except Exception as e:
                        if attempt < max_retries - 1:
                            wait = (attempt + 1) * 10
                            logger.warning(f"Erro ({e}). Tentando novamente em {wait}s...")
                            time.sleep(wait)
                        else:
                            logger.error(f"Falha definitiva após {max_retries} tentativas.")
                            params = None
                            break
                
                if not response_text:
                    break

                xml_cleaned = "".join(ch for ch in response_text if ch.isprintable() or ch in "\t\n\r")
                
                try:
                    data_dict = xmltodict.parse(xml_cleaned, process_namespaces=False)
                    res_root = data_dict.get('OAI-PMH', {})
                    
                    if 'error' in res_root:
                        logger.warning(f"OAI Error: {res_root['error']}")
                        break

                    list_records = res_root.get('ListRecords', {})
                    records = list_records.get('record', [])
                    if isinstance(records, dict): records = [records]

                    if records:
                        operations = []
                        for record in records:
                            identifier = record.get('header', {}).get('identifier')
                            metadata = record.get('metadata', {})
                            
                            if prefix == 'oai_dc' and 'oai_dc:dc' in metadata:
                                metadata = metadata['oai_dc:dc']
                            elif prefix == 'marcxml' and 'record' in metadata:
                                metadata = metadata['record']

                            if identifier and metadata:
                                clean_meta = {k: v for k, v in metadata.items() if not k.startswith('@')}
                                clean_meta['id'] = identifier
                                clean_meta['harvested_at'] = datetime.now()
                                operations.append(ReplaceOne({'id': identifier}, clean_meta, upsert=True))

                        if operations:
                            coll.bulk_write(operations, ordered=False)
                            logger.info(f"Salvos {len(operations)} registros.")

                    # resumptionToken (paginacao)
                    token_match = re.search(r'<resumptionToken[^>]*>(.*?)</resumptionToken>', response_text)
                    if token_match and token_match.group(1):
                        params = {
                            'verb': 'ListRecords',
                            'resumptionToken': token_match.group(1)
                        }
                    else:
                        params = None
                        logger.info(f"Concluído: {journal_id} ({prefix}).")

                except Exception as e:
                    logger.error(f"Erro ao processar XML: {e}")
                    break


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0
}
with DAG(
    'DH_01_ojs_to_mongodb',
    params={
        "from_date": Param(
            default=None,
            type=["null", "string"],
            maxLength=10,
        ),
        "journal_id": Param(
            default=None,
            type=["null", "integer"],
        ),
    },
    default_args=default_args,
    description='Data Harvesting - Harvesting de XML do OJS para o MongoDB',
    tags=["data_harvesting", "mongodb", "ojs", "journals"],
    schedule="0 21 * * 1-5",
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