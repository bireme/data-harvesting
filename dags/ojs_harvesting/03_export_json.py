import os
import csv
import json
import logging
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator


sources_cache = {}

def fetch_source_from_mysql(mysql_cursor, source_id):
    """Busca tit_serie, vol, num, issn no MySQL quando source_id já existia."""
    logger = logging.getLogger(__name__)
    if not mysql_cursor or not source_id:
        return "", "", "", ""
    
    if source_id in sources_cache:
        return sources_cache[source_id]
    
    try:
        query = """
            SELECT 
                a.title_serial, 
                a.volume_serial, 
                a.issue_number, 
                a.issn 
            FROM biblioref_referencesource AS a
            WHERE a.reference_ptr_id = %s
            LIMIT 1
        """
        mysql_cursor.execute(query, (source_id,))
        row = mysql_cursor.fetchone()
        if row:
            res = (row[0] or "", row[1] or "", row[2] or "", row[3] or "")
        else:
            res = ("", "", "", "")
    except Exception as err:
        logger.warning(f"Erro ao consultar fonte {source_id} no MySQL: {err}")
        res = ("", "", "", "")

    sources_cache[source_id] = res
    return res


def export_slices_to_json():
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    mongo_db = 'OJS_Transformed'
    
    file_export_path = Variable.get("OAI_DC_INPUT_PATH")
    os.makedirs(file_export_path, exist_ok=True)
    
    # --- CONEXÃO MYSQL FI-ADMIN PARA BUSCAR FONTES ---
    mysql_cursor = None
    mysql_conn = None
    try:
        mysql_hook = MySqlHook(mysql_conn_id='FI_ADMIN_DB')
        mysql_conn = mysql_hook.get_conn()
        mysql_cursor = mysql_conn.cursor()
    except Exception as e:
        logger.warning(f"Não foi possível conectar ao MySQL FI_ADMIN_DB: {e}")

    client = mongo_hook.get_conn()
    all_collections = client[mongo_db].list_collection_names()
    transformed_collections = [c for c in all_collections if c.endswith('_transformed')]
    
    logger.info(f"Há {len(transformed_collections)} coleções para exportar.")

    # --- CABECALHO CSV ---
    report_csv_path = os.path.join(file_export_path, "report.csv")
    csv_headers = [
        "Marcar /p Apagar", "ID do OJS", "ID do FONTE FI-ADMIN", 
        "Tipo de literatura", "Nível de Tratamento", "Data de Publicação", 
        "Ano de Publicação", "Título - Série", 
        "Volume", "Número", "ISSN", "Título - Artigo", "Página", "URL", 
        "Indexado em", "Palavra chave do Autor", "Resumo"
    ]
    
    with open(report_csv_path, 'w', encoding='utf-8', newline='') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=';')
        csv_writer.writerow(csv_headers)

        for coll_name in transformed_collections:
            logger.info(f"Exportando: {coll_name}")
            
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

                filepath = os.path.join(coll_folder_path, f"slice_{doc_id}.json")
                try:
                    with open(filepath, 'w', encoding='utf-8') as json_file:
                        json.dump(combined_doc, json_file, indent=2, ensure_ascii=False)
                except Exception as e:
                    logger.error(f"Falha ao escrever em slice_{doc_id}.json: {str(e)}")

                # --- EXTRACAO DE DADOS PARA O CSV ---
                id_ojs = result.get('id', '')
                id_fonte = ""
                tipo_lit = ""
                nivel_trat = ""
                data_pub = ""
                ano_pub = ""
                tit_serie = ""
                vol = ""
                num = ""
                issn = ""
                tit_artigo = ""
                pagina = ""
                url = ""
                indexado = ""
                palavras_chave = ""
                resumo = ""

                for item in combined_doc:
                    model = item.get('model', '')
                    fields = item.get('fields', {})

                    if model == 'biblioref.reference':
                        if fields.get('treatment_level') == 'as':
                            tipo_lit = fields.get('literature_type', tipo_lit)
                            nivel_trat = fields.get('treatment_level', nivel_trat)
                            data_pub = fields.get('publication_date_normalized', data_pub)
                            
                            if data_pub and len(data_pub) >= 4:
                                ano_pub = data_pub[:4]

                            ea = fields.get('electronic_address')
                            if ea:
                                try:
                                    ea_list = json.loads(ea) if isinstance(ea, str) else ea
                                    if isinstance(ea_list, list) and ea_list:
                                        url = ea_list[0].get('_u', '')
                                except Exception: pass

                            idx = fields.get('indexed_database')
                            if idx:
                                if isinstance(idx, list):
                                    indexado = "; ".join(str(i) for i in idx)
                                else:
                                    indexado = str(idx)

                            ak = fields.get('author_keyword')
                            if ak:
                                try:
                                    ak_list = json.loads(ak) if isinstance(ak, str) else ak
                                    if isinstance(ak_list, list):
                                        palavras_chave = "; ".join([str(k.get('text', '')) for k in ak_list if isinstance(k, dict)])
                                except Exception: pass

                            ab = fields.get('abstract')
                            if ab:
                                try:
                                    ab_list = json.loads(ab) if isinstance(ab, str) else ab
                                    if isinstance(ab_list, list):
                                        resumo = " | ".join([str(a.get('text', '')) for a in ab_list if isinstance(a, dict)])
                                except Exception: pass

                    elif model == 'biblioref.referenceanalytic':
                        id_fonte = fields.get('source', id_fonte)

                        titles = fields.get('title')
                        if titles:
                            try:
                                t_list = json.loads(titles) if isinstance(titles, str) else titles
                                if isinstance(t_list, list) and t_list:
                                    tit_artigo = str(t_list[0].get('text', ''))
                            except Exception: pass

                        pg = fields.get('pages')
                        if pg:
                            try:
                                pg_dict = json.loads(pg) if isinstance(pg, str) else pg
                                if isinstance(pg_dict, dict):
                                    s_page = pg_dict.get('start_page', '')
                                    e_page = pg_dict.get('end_page', '')
                                    eloc = pg_dict.get('elocation_id', '')
                                    if eloc:
                                        pagina = str(eloc)
                                    elif s_page and e_page:
                                        pagina = f"{s_page}-{e_page}"
                                    else:
                                        pagina = str(s_page)
                            except Exception:
                                pagina = str(pg)

                    elif model == 'biblioref.referencesource':
                        tit_serie = fields.get('title_serial', tit_serie)
                        vol = fields.get('volume_serial', vol)
                        num = fields.get('issue_number', num)
                        issn = fields.get('issn', issn)

                # --- SE OS DADOS DA FONTE NÃO ESTAVAM NO JSON, BUSCA NO MYSQL ---
                if id_fonte and not (tit_serie or vol or num or issn):
                    m_tit_serie, m_vol, m_num, m_issn = fetch_source_from_mysql(mysql_cursor, id_fonte)
                    tit_serie = m_tit_serie
                    vol = m_vol
                    num = m_num
                    issn = m_issn

                csv_row = [
                    "",                # Marcar /p Apagar
                    id_ojs,
                    id_fonte,
                    tipo_lit,
                    nivel_trat,
                    data_pub,
                    ano_pub,
                    tit_serie,
                    vol,
                    num,
                    issn,
                    tit_artigo,
                    pagina,
                    url,
                    indexado,
                    palavras_chave,
                    resumo
                ]
                csv_writer.writerow(csv_row)

    if mysql_cursor:
        mysql_cursor.close()
    if mysql_conn:
        mysql_conn.close()

    logger.info(f"Exportação concluída. JSONs salvos organizados por pastas em: {file_export_path}")
    logger.info(f"Relatório CSV salvo em: {report_csv_path}")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    'DH_03_ojs_export_slices',
    default_args=default_args,
    description='Data Harvesting - Exporta os documentos OJS em arquivos JSON organizados por coleções e gera relatório CSV',
    tags=["data_harvesting", "mongodb", "ojs", "export"],
    schedule=None,
    catchup=False
) as dag:
    export_slices_task = PythonOperator(
        task_id='export_slices_to_json',
        python_callable=export_slices_to_json
    )