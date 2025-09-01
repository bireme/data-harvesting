import json
import logging
import requests
from datetime import datetime, timedelta
from datetime import timezone

from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.filesystem import FSHook
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import Variable, Param
import os

from data_harvesting.dags.oai_dc_to_fi_admin.normalize_fields import (get_source_sas_id,
    normalize_lang_code, normalize_pagination, get_short_title_and_issn)

def get_doc_template(type='analytic'):
  template_source = [
    {
      "model" : "biblioref.reference",
      "fields": {
        "literature_type" : "S",
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
        "record_type" : "a",
        "treatment_level" : "as",
        "indexed_database" : [1],
        "BIREME_reviewed" : False,
        "cooperative_center_code" : "BR1.1",
        "created_by" : 2,
        "status" : -2,
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


def check_duplicate(journal_title, article_title, publication_year):
    """
    Check for duplicates using the deduplication service
    Returns: tuple (is_duplicate: bool, duplicate_info: dict or None)
    """
    dedup_service_url = Variable.get("DEDUP_SERVICE_URL", "https://dedup.bireme.org/services/get/duplicates")

    dedup_data = {
        'database': "lilacs_Sas",
        'schema': "LILACS_Sas_Three",
        'ano_publicacao': str(publication_year),
        'titulo_artigo': article_title,
        'titulo_revista': journal_title
    }

    try:
        logging.info(f"Checking for duplicates for article: {article_title}, journal: {journal_title}, year: {publication_year}")
        response = requests.get(dedup_service_url, params=dedup_data)
        response.raise_for_status()
        result = response.json()

        # Check if duplicates were found
        if result.get('total', 0) > 0 and result.get('result'):
            return True, result
        else:
            return False, None

    except Exception as e:
        logging.error(f"Error checking duplicates: {e}")
        # On error, assume no duplicate to avoid blocking the process
        return False, None

def write_duplicate_json_file(file_export_path, original_record, duplicate_info):
    """Write duplicate information to JSON file"""
    duplicate_log_path = f"{file_export_path}/duplicates.json"

    # Remove _id field from original record to avoid serialization issues
    if '_id' in original_record:
        del original_record['_id']
        del original_record['_metadata']

    duplicate_record = {
        'original_record': original_record,
        'duplicate_info': duplicate_info
    }

    try:
        with open(duplicate_log_path, 'a') as log_file:
            json.dump(duplicate_record, log_file, indent=2)
            log_file.write('\n')
    except Exception as e:
        logging.error(f"Error writing duplicate log: {e}")


@dag(
    'DG_02_export_oai_dc_to_fi_admin',
	default_args={
		'owner': 'airflow',
		'start_date': datetime(2025, 1, 1),
		'retries': 1,
		'retry_delay': timedelta(minutes=5),
	},
	description='Parse OAI-DC bibliographic references in MongoDB and export JSON files (FIADMIN schema)',
    params={
        "mongodb_collection": Param("oai_dc_records", type="string"),
        "export_path": Param(f"{Variable.get('OAI_DC_INPUT_PATH')}fi_admin_import", type="string"),
    },
	schedule_interval=None,
    tags=['oai-dc', 'export', 'mongodb', 'fi-admin'],
    doc_md="""
    ### Required Connections
    - **MONGODB_OAI_DC**: Connection to the MongoDB database where the records will be read.
    - **FI_ADMIN_DB**: Connection to FI-ADMIN database (used for normalize and include additional data).

    ### Required Variables
    - **DEDUP_SERVICE_URL**: URL for the deduplication service API.
    - **OAI_DC_INPUT_PATH**: Path to the directory containing the OAI-DC XML files.

    Please ensure these are set in the Airflow Admin UI.
    """
)
def export_oai_dc_to_fi_admin():

    @task()
    def get_fiadmin_last_id():
        # Make an HTTP request to retrieve the last_id from the FIADMIN database
        url = "https://fi-admin-api.bvsalud.org/api/bibliographic/get_last_id/?format=json"
        response = requests.get(url)
        response.raise_for_status()
        last_id = response.json()
        try:
            numeric_last_id = int(last_id)
        except Exception as err:
            raise AirflowException("Invalid last_id format received from FIADMIN API") from err
        if numeric_last_id <= 0:
            raise AirflowException("The retrieved fi-admin last_id must be greater than zero")

        logging.info(f"Retrieved fi-admin last_id: {last_id}")
        return last_id


    @task()
    def export_biblioref(last_id: str, **context):
        logger = logging.getLogger("airflow.task")
        # Constants
        MONGO_CONN_ID = 'MONGODB_OAI_DC'
        MONGO_COLLECTION = context['params']['mongodb_collection']

        mongo_hook = MongoHook(MONGO_CONN_ID)
        fiadmindb_hook = MySqlHook(mysql_conn_id='FI_ADMIN_DB')
        fiadmindb_conn = fiadmindb_hook.get_conn()
        increment_id_by = Variable.get("biblioref_increment_id_by", 100)

        # Export path = subdirectory "fi_admin_export"
        file_export_path = context['params']['export_path']

        # Check if export path exists, create it if it doesn't
        if not os.path.exists(file_export_path):
            os.makedirs(file_export_path)
            logger.info(f"Created export directory: {file_export_path}")
        else:
            logger.info(f"Export directory already exists: {file_export_path}")


        now = datetime.now(timezone.utc).astimezone().isoformat()
        next_id = int(last_id) + int(increment_id_by)

        local_source_list = {}
        duplicate_count = 0
        exported_count = 0
        error_count = 0

        results = mongo_hook.find(MONGO_COLLECTION, {})
        for result in results:
            # Initialize reference information for error logging
            record_id = result.get('_id', 'unknown')
            record_identifier = result.get('id', 'unknown')

            try:
                next_id += 1
                doc = get_doc_template('analytic')
                doc_source = get_doc_template('source')

                # Set the common fields for both documents
                doc[0]['fields']['created_time'] = now
                doc_source[0]['fields']['created_time'] = now
                doc[0]['fields']['updated_time'] = now
                doc_source[0]['fields']['updated_time'] = now

                interoperability_source = json.dumps({'_': 'HARVEST_DATA', '_b': MONGO_COLLECTION, '_i': result['id']})
                doc[0]['fields']['interoperability_source'] = interoperability_source
                doc_source[0]['fields']['interoperability_source'] = interoperability_source

                if 'abstracts' in result:
                    doc[0]['fields']['abstract'] = result['abstracts']

                if 'subjects' in result:
                    doc[0]['fields']['author_keyword'] = result['subjects']

                if 'language' in result:
                    language = normalize_lang_code(result['language'])
                    doc[0]['fields']['text_language'] = json.dumps([language])

                if 'subjects' in result:
                    normalized_subjects = []
                    for subject in result['subjects']:
                        subject['_i'] = normalize_lang_code(subject['_i'])
                        normalized_subjects.append(subject)
                    doc[0]['fields']['author_keyword'] = normalized_subjects

                if 'date' in result:
                    doc[0]['fields']['publication_date_normalized'] = result['date']

                if 'fulltext_url' in result:
                    doc[0]['fields']['electronic_address'] = json.dumps([{'_u': result['fulltext_url']}])

                if 'volume' in result:
                    doc_source[1]['fields']['volume_serial'] = result['volume']

                if 'number' in result:
                    doc_source[1]['fields']['issue_number'] = result['number']

                # if 'publishers' in result:
                #     doc[1]['fields']['publisher'] = result['publishers'][0]["text"]

                if 'titles' in result:
                    normalized_titles = []
                    for title in result['titles']:
                        title['_i'] = normalize_lang_code(title['_i'])
                        normalized_titles.append(title)
                    doc[1]['fields']['title'] = normalized_titles

                if 'doi' in result:
                    doc[1]['fields']['doi_number'] = result['doi']

                if 'pagination' in result:
                    doc[1]['fields']['pages'] = json.dumps(normalize_pagination(result['pagination']))

                if 'creators' in result:
                    doc[1]['fields']['individual_author'] = []
                    for author in result['creators']:
                        doc[1]['fields']['individual_author'].append({'text': author})
                    doc[1]['fields']['individual_author'] = json.dumps(doc[1]['fields']['individual_author'])

                if 'journal' in result and 'issn' in result:
                    journal_issn = None
                    for issn in result['issn']:
                        journal_issn = get_short_title_and_issn(fiadmindb_conn, result['journal'], issn)
                        if journal_issn:
                            doc_source[1]['fields']['title_serial'] = journal_issn['journal']
                            doc_source[1]['fields']['issn'] = journal_issn['issn']
                            logger.info(f"Title info found in database for journal: {journal_issn['journal']}, ISSN: {journal_issn['issn']}")
                            break

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
                title_analytic = result['titles'][0]['text']
                ref_volume_number = ""
                if volume and number:
                    ref_volume_number = f"; {volume}({number})"
                elif volume:
                    ref_volume_number = f"; {volume}"

                reference_title = f"{journal}{ref_volume_number}, {year} | {title_analytic}"
                doc[0]['fields']['reference_title'] = reference_title

                # Check if the reference is present in the duplicated service index
                is_duplicate = False
                is_duplicate, duplicate_info = check_duplicate(journal, title_analytic, year)

                if is_duplicate:
                    # Write duplicate info to CSV instead of exporting
                    dup_id = result.get('_id')
                    write_duplicate_json_file(file_export_path, result, duplicate_info)
                    duplicate_count += 1
                    logger.info(f"Duplicate found for record {dup_id}, added to duplicates.json")
                    continue

                # check for source id localy and in fiadmin
                source_local_key = f"{journal}-{year}-{volume}-{number}"
                source_id = local_source_list.get(source_local_key)

                if not source_id:
                    source_id = get_source_sas_id(fiadmindb_conn, journal, year, volume, number)
                    if source_id:
                        # save source id in local list to avoid duplicate queries in fiadmindb
                        local_source_list[source_local_key] = source_id
                        logger.info(f"Source ID found in FIADMIN db for: {journal}, {year}, {volume}, {number}")
                else:
                    logger.info(f"Source ID found localy for: {journal}, {year}, {volume}, {number}")

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

                # Save the combined document to a JSON file
                filepath = f"{file_export_path}/slice_{result['_id']}.json"
                with open(filepath, 'w') as json_file:
                    json.dump(combined_doc, json_file, indent=2)

                exported_count += 1

            except Exception as e:
                # Log detailed error information for debugging
                error_context = {
                    'record_id': str(record_id),
                    'record_identifier': str(record_identifier),
                    'reference_title': reference_title,
                    'error_type': type(e).__name__,
                    'error_message': str(e)
                }

                logger.error(f"Error processing record {record_id} (identifier: {record_identifier}): {e}")

                # Write error details to a separate log file for manual review
                error_log_path = f"{file_export_path}/processing_errors.json"
                error_record = {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'error_context': error_context
                }

                try:
                    with open(error_log_path, 'a') as error_file:
                        json.dump(error_record, error_file, indent=2)
                        error_file.write('\n')
                except Exception as log_error:
                    logger.error(f"Failed to write error log for record {record_id}: {log_error}")

                # Continue processing the next record instead of crashing
                error_count += 1
                continue

        logger.info(f"References exported: {exported_count}, duplicated: {duplicate_count}, errors: {error_count}")

    # Control task execution
    last_id = get_fiadmin_last_id()
    export_biblioref(last_id)

# Instantiate the DAG
dag = export_oai_dc_to_fi_admin()