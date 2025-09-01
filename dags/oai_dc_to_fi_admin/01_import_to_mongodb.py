import re
import os
import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.decorators import task_group
from airflow.utils.dates import timezone
from airflow.models import Variable, Param

from data_harvesting.dags.oai_dc_to_fi_admin.common import (
    is_doi,
    is_article_url,
    is_issn,
    setup_mongodb,
    insert_into_mongodb,
    duplicate_collection
)

# Initialize logger globally for the DAG
logger = logging.getLogger("airflow.task")


@dag(
    'DG_01_import_oai_dc_to_mongodb',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Parse bibliographic records in XML format and import into MongoDB collection',
    params={
        "oai_dc_input_path": Param(Variable.get("OAI_DC_INPUT_PATH"), type="string"),
        "mongodb_collection": Param("oai_dc_records", type="string")
    },
    schedule_interval=None,
    tags=['oai-dc', 'import', 'mongodb'],
    doc_md="""
    ### Required Connections
    - **MONGODB_OAI_DC**: Connection to the MongoDB database where the records will be imported.

    ### Required Variables
    - **OAI_DC_INPUT_PATH**: Path to the directory containing the XML files.

    Please ensure these are set in the Airflow Admin UI.
    """
)
def import_oai_dc_to_mongodb():
    ns = {
        'oai': "http://www.openarchives.org/OAI/2.0/",
        'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
        'dc': 'http://purl.org/dc/elements/1.1/',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        'xml': 'http://www.w3.org/XML/1998/namespace'
    }

    @task()
    def parse_xml_files(**kwargs):
        directory_path = kwargs['params']['oai_dc_input_path']
        records_info = []

        # First, process all XML files in the current directory
        for item in os.listdir(directory_path):
            item_path = os.path.join(directory_path, item)

            # Process XML files in the current directory
            if os.path.isfile(item_path) and item.endswith(".xml"):
                logger.info(f"Adding XML file to processing queue: {item_path}")
                records_info.extend(parse_single_xml(item_path))

        # Then, process XML files in subdirectories
        for folder_name in os.listdir(directory_path):
            folder_path = os.path.join(directory_path, folder_name)

            # Check if the item in the directory is a subdirectory
            if os.path.isdir(folder_path):
                for xml_file in os.listdir(folder_path):
                    if xml_file.endswith(".xml"):
                        xml_file_path = os.path.join(folder_path, xml_file)

                        # Parse each XML file
                        logger.info(f"Adding XML file to processing queue: {xml_file_path}")
                        records_info.extend(parse_single_xml(xml_file_path))

        return records_info


    def parse_sources(sources):
        record_info = {}

        for source in sources:
            if is_issn(source.text):
                if 'issn' in record_info:
                    record_info['issn'].append(source.text)
                else:
                    record_info['issn'] = [source.text]
            else:
                source_val = {
                    'text': source.text,
                    '_i': source.get(f'{{{ns["xml"]}}}lang')
                }
                if 'source' in record_info:
                    record_info['sources'].append(source_val)
                else:
                    record_info['sources'] = [source_val]

                source_parts = source_val['text'].split(';')

                if len(source_parts) == 3:
                    record_info['pagination'] = source_parts.pop().strip()
                record_info['journal'] = source_parts.pop(0).strip()

                vol_num_year = source_parts.pop(0).strip()
                pattern = r'(?:Vol\.|v\.)\s*(?P<volume>\d+)\s*(?:(?:No\.|N[úu]m\.|n\.)\s*(?P<number>\d+)\s*)?\((?P<year>\d{4})\)'
                match = re.match(pattern, vol_num_year)
                if match:
                    record_info['volume'] = match.group('volume').strip()
                    record_info['year'] = match.group('year').strip()

                    if match.group('number'):
                        record_info['number'] = match.group('number').strip()

        return record_info


    def parse_identifiers(identifiers):
        record_info = {}

        for identifier in identifiers:
            if is_doi(identifier.text):
                record_info['doi'] = identifier.text
            elif is_article_url(identifier.text):
                record_info['fulltext_url'] = identifier.text
            else:
                if 'identifier' in record_info:
                    record_info['identifier'].append(identifier.text)
                else:
                    record_info['identifier'] = [identifier.text]

        return record_info


    def parse_single_xml(xml_file_path):
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
        except ET.ParseError as e:
            logger.error(f"Error parsing XML file {xml_file_path}: {e}")
            return []

        records_info = []
        records = root.findall('.//oai:record', namespaces=ns)
        for record in records:
            try:
                # Skip if record deleted
                status = record.find(".//oai:header", namespaces=ns).get("status")
                if status and status == "deleted":
                    continue

                record_info = {}

                dc = record.find('./oai:metadata/oai_dc:dc', namespaces=ns)

                # Multi occ fields
                record_info['creators'] = [creator.text for creator in dc.findall('./dc:creator', namespaces=ns)]
                record_info['types'] = [type.text for type in dc.findall('./dc:type', namespaces=ns)]

                # Single occ fields
                record_info['id'] = record.find('./oai:header/oai:identifier', namespaces=ns).text
                record_info['date'] = dc.find('./dc:date', namespaces=ns).text if dc.find('./dc:date', namespaces=ns) is not None else None
                record_info['language'] = dc.find('./dc:language', namespaces=ns).text if dc.find('./dc:language', namespaces=ns) is not None else None

                # Handle identifier tag
                record_info.update(
                    parse_identifiers(dc.findall('./dc:identifier', namespaces=ns))
                )

                # Handle source tag
                record_info.update(
                    parse_sources(dc.findall('./dc:source', namespaces=ns))
                )

                # Multi occ fields with lang
                tags_with_lang = [
                    {
                        "name": 'titles',
                        "xpath": './dc:title'
                    },
                    {
                        "name": 'subjects',
                        "xpath": './dc:subject'
                    },
                    {
                        "name": 'abstract',
                        "xpath": './dc:description'
                    },
                    {
                        "name": 'publishers',
                        "xpath": './dc:publisher'
                    }
                ]
                for tag_with_lang in tags_with_lang:
                    # Collect both content and language
                    tags = dc.findall(tag_with_lang['xpath'], namespaces=ns)
                    if tags:
                        record_info[tag_with_lang['name']] = [
                            {
                                'text': tag.text,
                                '_i': tag.get(f'{{{ns["xml"]}}}lang')
                            } for tag in tags
                        ]

                # Add metadata info
                record_info['_metadata'] = {
                    'xml_path': xml_file_path,
                    'xml_filename': os.path.basename(xml_file_path),
                    'xml_last_modified': timezone.convert_to_utc(datetime.fromtimestamp(os.path.getmtime(xml_file_path))),
                    'imported_date': timezone.utcnow()
                }

                records_info.append(record_info)
            except Exception as e:
                logger.error(f"Error processing record in {xml_file_path}: {e}")

                error_context = {
                    'xml_file_path': xml_file_path,
                    'record_id': record_info.get('id'),
                    'error_message': str(e)
                }
                xml_dir = os.path.dirname(xml_file_path)
                # Write error details to a separate log file for manual review
                error_log_path = f"{xml_dir}/processing_errors.json"
                error_record = {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'error_context': error_context
                }

                try:
                    with open(error_log_path, 'a') as error_file:
                        json.dump(error_record, error_file, indent=2)
                        error_file.write('\n')
                except Exception as log_error:
                    logger.error(f"Failed to write error log at {error_log_path}: {log_error}")


        return records_info


    # Constants
    MONGO_CONN_ID = 'MONGODB_OAI_DC'
    TRANSFORM_COLLECTION = 'transform'

    # Task dependencies
    @task_group()
    def extract():
        @task()
        def get_mongodb_collection(**context):
            # Get the mongodb_collection parameter from DAG params
            return context['params']['mongodb_collection']

        mongodb_collection_task = get_mongodb_collection()
        records_info_task = parse_xml_files()
        setup_mongodb_task = setup_mongodb(MONGO_CONN_ID, mongodb_collection_task)
        insert_output_task = insert_into_mongodb(records_info_task, MONGO_CONN_ID, mongodb_collection_task)
        # promote_to_transform = duplicate_collection(mongodb_collection_task, TRANSFORM_COLLECTION, MONGO_CONN_ID)

        records_info_task >> insert_output_task
        setup_mongodb_task >> insert_output_task
        # insert_output_task >> promote_to_transform

    extract()


# Accessing the generated DAG
xml_dag = import_oai_dc_to_mongodb()
