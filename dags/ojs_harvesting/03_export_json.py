import os
import json
import logging
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator


def export_slices_to_json():
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    mongo_db = 'OJS_Transformed'
    
    file_export_path = Variable.get("OAI_DC_INPUT_PATH")
    os.makedirs(file_export_path, exist_ok=True)
    
    client = mongo_hook.get_conn()
    all_collections = client[mongo_db].list_collection_names()
    transformed_collections = [c for c in all_collections if c.endswith('_transformed')]
    
    logger.info(f"Há {len(transformed_collections)} coleções para exportar.")

    for coll_name in transformed_collections:
        logger.info(f"Exportando: {coll_name}")
        
        # Cria a subpasta específica para a collection se ela não existir
        coll_folder_path = os.path.join(file_export_path, coll_name)
        os.makedirs(coll_folder_path, exist_ok=True)
        
        collection = mongo_hook.get_collection(coll_name, mongo_db=mongo_db)
        records = collection.find({})
        
        for result in records:
            combined_doc = result.get('data')
            doc_id = result.get('_id')
            
            if not combined_doc:
                logger.warning(f"Documento com _id {doc_id} sem 'data'. Pulando.")
                continue

            # Constrói o caminho final apontando para a subpasta da collection
            filepath = os.path.join(coll_folder_path, f"slice_{doc_id}.json")
            
            try:
                with open(filepath, 'w', encoding='utf-8') as json_file:
                    json.dump(combined_doc, json_file, indent=2, ensure_ascii=False)
            except Exception as e:
                logger.error(f"Falha ao escrever em slice_{doc_id}.json: {str(e)}")

    logger.info(f"Exportação concluída. JSONs salvos organizados por pastas em: {file_export_path}")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}
with DAG(
    'DH_03_ojs_export_slices',
    default_args=default_args,
    description='Data Harvesting - Exporta os documentos OJS em arquivos JSON organizados por coleções',
    tags=["data_harvesting", "mongodb", "ojs", "export"],
    schedule=None,
    catchup=False
) as dag:
    export_slices_task = PythonOperator(
        task_id='export_slices_to_json',
        python_callable=export_slices_to_json
    )