import logging
import re
import json
from datetime import datetime
from datetime import timezone
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from data_harvesting.dags.ojs_harvesting.common import check_duplicate
from data_harvesting.dags.ojs_harvesting.common import get_journal_data
from data_harvesting.dags.ojs_harvesting.common import get_fiadmin_last_id
from data_harvesting.dags.ojs_harvesting.normalize_fields import parse_multilang_field
from data_harvesting.dags.ojs_harvesting.normalize_fields import normalize_lang_code
from data_harvesting.dags.ojs_harvesting.normalize_fields import normalize_pagination
from data_harvesting.dags.ojs_harvesting.normalize_fields import get_source_sas_id
from data_harvesting.dags.ojs_harvesting.normalize_fields import parse_sources
from data_harvesting.dags.ojs_harvesting.normalize_fields import parse_relations
from data_harvesting.dags.ojs_harvesting.normalize_fields import parse_identifiers


def get_doc_template(type='analytic'):
    template_source = [
        {
            "model" : "biblioref.reference",
            "fields": {
            "literature_type" : "S",
            "type_of_journal" : "p",
            "indexed_database" : [1],
            "BIREME_reviewed" : False,
            "cooperative_center_code" : "BR1.1",
            "created_by" : 2,
            "status" : 0,
            }
        },
        {
            "model" : "biblioref.referencesource",
            "fields": {}
        }
    ]

    template_analytic = [
        {
            "model" : "biblioref.reference",
            "fields": {
            "item_form": "s",
            "literature_type" : "S",
            "type_of_journal" : "p",
            "record_type" : "a",
            "treatment_level" : "as",
            "indexed_database" : [1],
            "BIREME_reviewed" : False,
            "cooperative_center_code" : "BR1.1",
            "created_by" : 2,
            "status" : 0,
            "software_version" : "BIREME HARVEST 0.1"
            }
        },
        {
            "model" : "biblioref.referenceanalytic",
            "fields": {}
        },
    ]
    template = template_analytic if type == 'analytic' else template_source

    return template


def setup_ojs_transformed():
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    mongo_db = 'OJS_Transformed'
    
    client = mongo_hook.get_conn()
    
    # Drop database if it exists
    if mongo_db in client.list_database_names():
        logger.info(f"Dropping existing database: {mongo_db}")
        client.drop_database(mongo_db)


def get_marcxml_data(mongo_db, journal_id, record_id):
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')

    marc_coll_name = f"{journal_id}_marcxml"
    marc_coll = mongo_hook.get_collection(marc_coll_name, mongo_db=mongo_db)
    marc_record = marc_coll.find_one({"id": record_id})
    
    authors = []
    if marc_record:
        datafields = marc_record.get('datafield', [])
        if isinstance(datafields, dict):
            datafields = [datafields]

        for field in datafields:
            tag = field.get('@tag')
            if tag in ["100", "720"]:
                subfields = field.get('subfield', [])
                if isinstance(subfields, dict):
                    subfields = [subfields]
                
                author_entry = {}
                for sub in subfields:
                    code = sub.get('@code')
                    text = sub.get('#text', '')
                    
                    if code == 'a':
                        author_entry['text'] = text
                    elif code == 'u':
                        author_entry['_1'] = text
                    elif code == '0':
                        if 'orcid.org' in text:
                            author_entry['_k'] = text
                        elif 'lattes.cnpq.br' in text:
                            author_entry['_l'] = text
                        elif '@' in text:
                            author_entry['_e'] = text
                
                if author_entry.get('text'):
                    authors.append(author_entry)
    
    return authors


def transform_ojs_data():
    logger = logging.getLogger(__name__)
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    mongo_db = 'OJS'
    all_collections = mongo_hook.get_conn()[mongo_db].list_collection_names()
    collections = [c for c in all_collections if c.endswith('_oai_dc')]
    logger.info(f"Encontradas {len(collections)} coleções no banco {mongo_db}.")

    fiadmindb_hook = MySqlHook(mysql_conn_id='FI_ADMIN_DB')
    fiadmindb_conn = fiadmindb_hook.get_conn()

    now = datetime.now(timezone.utc).astimezone().isoformat()

    last_id = get_fiadmin_last_id()
    next_id = int(last_id) + 100

    local_source_list = {}

    for coll_name in collections:
        logger.info(f"Processando coleção: {coll_name}")
        collection = mongo_hook.get_collection(coll_name, mongo_db=mongo_db)

        journal_id = coll_name.split('_')[0]
        journal_data = get_journal_data(journal_id)
        journal_issn = journal_data.get('issn')
        journal_title = journal_data.get('journal')

        if not journal_title:
            continue
        
        records = collection.find({})
        for record in records:
            doc = get_doc_template('analytic')
            doc_source = get_doc_template('source')

            doc[0]['fields']['created_time'] = now
            doc_source[0]['fields']['created_time'] = now
            doc[0]['fields']['updated_time'] = now
            doc_source[0]['fields']['updated_time'] = now

            interoperability_source = json.dumps({'_': 'HARVEST_DATA', '_b': coll_name, '_i': record['id']})
            doc[0]['fields']['interoperability_source'] = interoperability_source
            doc_source[0]['fields']['interoperability_source'] = interoperability_source

            doc_source[1]['fields']['title_serial'] = journal_title
            doc_source[1]['fields']['issn'] = journal_issn

            if 'dc:date' in record:
                doc[0]['fields']['publication_date_normalized'] = record['dc:date']

            if 'dc:language' in record:
                language = normalize_lang_code(record['dc:language'])
                doc[0]['fields']['text_language'] = json.dumps([language])

            if 'dc:relation' in record:
                fulltext_url = parse_relations(record['dc:relation'])
                if fulltext_url:
                    doc[0]['fields']['electronic_address'] = fulltext_url

            if 'dc:identifier' in record:
                identifiers = parse_identifiers(record['dc:identifier'])
                if 'doi' in identifiers:
                    doc[1]['fields']['doi_number'] = identifiers['doi']
                    doc_source[1]['fields']['doi_number'] = identifiers['doi']

            if 'dc:description' in record:
                abstracts = parse_multilang_field(record['dc:description'])
                if abstracts:
                    doc[0]['fields']['abstract'] = abstracts

            if 'dc:title' in record:
                titles = parse_multilang_field(record['dc:title'])
                if titles:
                    doc[1]['fields']['title'] = titles

            if not 'title' in doc[1]['fields']:
                continue

            marc_record = get_marcxml_data(mongo_db, journal_id, record['id'])
            if marc_record:
                doc[1]['fields']['individual_author'] = json.dumps(marc_record)

            if 'dc:subject' in record:
                subjects = parse_multilang_field(record['dc:subject'])
                if subjects:
                    doc[0]['fields']['author_keyword'] = subjects

            if 'dc:source' in record:
                sources = parse_sources(record['dc:source'])

                if 'doi' in sources:
                    doc[1]['fields']['doi_number'] = sources['doi']
                    doc_source[1]['fields']['doi_number'] = sources['doi']

                if 'volume' in sources:
                    doc_source[1]['fields']['volume_serial'] = sources['volume']

                if 'number' in sources:
                    doc_source[1]['fields']['issue_number'] = sources['number']

                if 'pagination' in sources:
                    pages = normalize_pagination(sources['pagination'])
                    if pages:
                        doc[1]['fields']['pages'] = json.dumps(pages)

            # Initialize journal, year, volume, and number
            journal = ""
            year = ""
            volume = ""
            number = ""

            if 'title_serial' in doc_source[1]['fields'] and 'publication_date_normalized' in doc[0]['fields']:
                journal = doc_source[1]['fields']['title_serial']
                year = doc[0]['fields']['publication_date_normalized'][:4]

            if 'volume_serial' in doc_source[1]['fields']:
                volume = doc_source[1]['fields']['volume_serial']
            if 'issue_number' in doc_source[1]['fields']:
                number = doc_source[1]['fields']['issue_number']

            # add reference_title
            title_analytic = doc[1]['fields']['title'][0]['text']
            ref_volume_number = ""
            if volume and number:
                ref_volume_number = f"; {volume}({number})"
            elif volume:
                ref_volume_number = f"; {volume}"

            reference_title = f"{journal}{ref_volume_number}, {year} | {title_analytic}"
            doc[0]['fields']['reference_title'] = reference_title

            # Check if the reference is present in the duplicated service index
            is_duplicate, duplicate_info = check_duplicate(journal, title_analytic, year)
            if is_duplicate:
                continue

            next_id += 1

            # check for source id localy and in fiadmin
            source_local_key = f"{journal}-{year}-{volume}-{number}"
            source_id = local_source_list.get(source_local_key)

            if not source_id:
                source_id = get_source_sas_id(fiadmindb_conn, journal, year, volume, number)
                if source_id:
                    # save source id in local list to avoid duplicate queries in fiadmindb
                    local_source_list[source_local_key] = source_id

            if source_id:
                doc[1]['fields']['source'] = source_id
            else:
                # set primary key for the source document
                doc_source[0]['pk'] = next_id
                doc_source[1]['pk'] = next_id

                reference_title = f"{journal}{ref_volume_number}, {year}"
                doc_source[0]['fields']['reference_title'] = reference_title
                if year:
                    doc_source[0]['fields']['publication_date'] = year
                    doc_source[0]['fields']['publication_date_normalized'] = f"{year}0000"

                # set source field at analytic document
                doc[1]['fields']['source'] = next_id

                # create a local entry for the new source id
                logger.info(f"Source ID [{next_id}] created for: {journal}, {year}, {volume}, {number}")
                local_source_list[source_local_key] = next_id

                # increment next_id for the analytic document
                next_id += 1

            # Set the primary key for the analytic
            doc[0]['pk'] = next_id
            doc[1]['pk'] = next_id

            # Combine doc and doc_source if is a new source (not found in fiadmin or localy)
            combined_doc = doc
            if not source_id:
                combined_doc = doc_source + doc

            # Submit the combined_doc to OJS_Transformed database
            transformed_coll_name = f"{journal_id}_transformed"
            transformed_coll = mongo_hook.get_collection(transformed_coll_name, mongo_db='OJS_Transformed')
            transformed_coll.create_index("id", unique=True)
            
            # Use the original OJS record ID as the unique identifier for the transformed document
            transformed_record = {
                "id": record['id'],
                "data": combined_doc,
                "transformed_at": now
            }
            
            transformed_coll.replace_one(
                {'id': record['id']},
                transformed_record,
                upsert=True
            )


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0
}
with DAG(
    'DH_02_ojs_transformation',
    default_args=default_args,
    description='Data Harvesting - Transformation de dados do OJS para o formato do FIADMIN',
    tags=["data_harvesting", "mongodb", "ojs", "journals"],
    schedule=None,
    catchup=False
) as dag:
    transform_ojs_data_task = PythonOperator(
        task_id='transform_ojs_data',
        python_callable=transform_ojs_data
    )
    setup_ojs_transformed_task = PythonOperator(
        task_id='setup_ojs_transformed',
        python_callable=setup_ojs_transformed
    )

    setup_ojs_transformed_task >> transform_ojs_data_task
