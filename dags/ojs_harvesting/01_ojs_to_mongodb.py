import logging
import re
import requests
import xmltodict
from datetime import datetime
import time
from datetime import timedelta
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from pymongo import ReplaceOne


def list_ojs_journals():
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('ojs_oai_sources', 'TITLE')
    
    query = {
        "in_title": True
    }
    projection = {"journal_id": 1, "oai_url": 1, "_id": 0}
    results = collection.find(query, projection)
    
    journals = list(results)
    logger.info(f"{len(journals)} periódicos encontrados.")
    
    return [[journals[i:i + 5]] for i in range(0, len(journals), 5)]


def harvest_oai_url(journals):
    logger = logging.getLogger(__name__)

    mongo_hook = MongoHook(mongo_conn_id='mongo')

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebkit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
    }
    metadata_prefixes = ['oai_dc', 'marcxml']
    mongo_db = 'OJS'

    for journal in journals:
        journal_id = journal.get('journal_id')
        oai_url = journal.get('oai_url')

        for prefix in metadata_prefixes:
            logger.info(f"Processando periódico {journal_id} com prefixo {prefix}.")
            collection_name = f"{journal_id}_{prefix}"

            # Ensure unique index on 'id' exists
            coll = mongo_hook.get_collection(collection_name, mongo_db=mongo_db)
            coll.create_index("id", unique=True)
            
            params = {
                'verb': 'ListRecords',
                'metadataPrefix': prefix
            }
            while params:
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        response = requests.get(oai_url, params=params, headers=headers, timeout=30)
                        response.raise_for_status()
                        break
                    except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError, requests.exceptions.ReadTimeout) as e:
                        if attempt < max_retries - 1:
                            wait_time = (attempt + 1) * 5
                            logger.warning(f"Erro de conexão ({e}). Tentando novamente em {wait_time}s...")
                            time.sleep(wait_time)
                        else:
                            raise
                
                # Clean "dirty" XML by removing non-printable control characters except tab, newline, return
                xml_text = response.text
                xml_cleaned = "".join(ch for ch in xml_text if ch.isprintable() or ch in "\t\n\r")
                try:
                    data_dict = xmltodict.parse(xml_cleaned, process_namespaces=False)
                except Exception as e:
                    logger.error(f"Erro ao parsear XML para {journal_id} ({prefix}): {e}")
                    logger.info(f"XML problemático (primeiros 500 caracteres): {xml_text[:500]}")
                    break
                
                # Extrai os registros individuais
                records = data_dict.get('OAI-PMH', {}).get('ListRecords', {}).get('record', [])
                if isinstance(records, dict):  # Caso venha apenas um registro como dict
                    records = [records]

                if records:
                    operations = []
                    for record in records:
                        identifier = record.get('header', {}).get('identifier')
                        metadata = record.get('metadata', {})
                        
                        if identifier and metadata:
                            if prefix == 'oai_dc' and 'oai_dc:dc' in metadata:
                                metadata = metadata['oai_dc:dc']
                            elif prefix == 'marcxml' and 'record' in metadata:
                                metadata = metadata['record']

                            # Remove '@' (XML namespaces)
                            metadata = {k: v for k, v in metadata.items() if not k.startswith('@')}

                            metadata['id'] = identifier
                            metadata['harvested_at'] = datetime.now()
                            operations.append(ReplaceOne({'id': identifier}, metadata, upsert=True))

                    if operations:
                        coll.bulk_write(operations)
                        logger.info(f"Inseridos {len(operations)} registros para {journal_id} ({prefix}).")

                # Verifica se existe um resumptionToken para paginação
                token_match = re.search(r'<resumptionToken[^>]*>(.*?)</resumptionToken>', response.text)
                if token_match and token_match.group(1):
                    params = {'verb': 'ListRecords', 'resumptionToken': token_match.group(1)}
                    logger.info(f"Token de retomada encontrado para {journal_id} ({prefix}), buscando próxima página.")
                else:
                    params = None
                    logger.info(f"Harvesting concluído para {journal_id} ({prefix}).")

                    

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0
}
with DAG(
    'DH_01_ojs_to_mongodb',
    default_args=default_args,
    description='Data Harvesting - Harvesting de XML do OJS para o MongoDB',
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