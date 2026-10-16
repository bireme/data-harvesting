from datetime import datetime, timedelta
import csv
import logging
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook


def extract_field_value(fields_array, field_code, subfield_code):
    for field_obj in fields_array:
        if field_code in field_obj:
            subfields = field_obj[field_code].get('subfields', [])
            for sub in subfields:
                if subfield_code in sub:
                    return sub[subfield_code]
    return None


def compare_issn_records(**context):
    logger = logging.getLogger("airflow.task")

    db_name = 'TITLE'
    mongo_hook = MongoHook(mongo_conn_id="mongo")
    client = mongo_hook.get_conn()
    db = client[db_name]

    portal_issn_coll = db['portal_issn']
    current_coll = db['current']

    discrepancies = []
    for record in portal_issn_coll.find({}):
        fields = record.get('fields', [])

        # Fetch ISSN from field '022', subfield 'a'
        issn = extract_field_value(fields, '022', 'a')
        if not issn:
            continue

        # Try to match with database 'TITLE', collection 'current', in field 'issn'
        current_record = current_coll.find_one({'issn': issn})

        if current_record:
            # Fetch fields from portal_issn
            portal_title = extract_field_value(fields, '222', 'a')
            portal_subtitle = extract_field_value(fields, '222', 'b')

            # Get corresponding values from current collection
            current_title = current_record.get('title')
            current_subtitle = current_record.get('subtitle')

            diffs = {}

            # Compare title only if portal_issn has a value and it differs from current
            if portal_title and portal_title != current_title:
                diffs['title'] = portal_title

            # Compare subtitle only if portal_issn has a value and it differs from current
            if portal_subtitle and portal_subtitle != current_subtitle:
                diffs['subtitle'] = portal_subtitle

            # If any comparison is different, record it
            if diffs:
                row_data = {'issn': issn}
                row_data.update(diffs)
                discrepancies.append(row_data)

    # If no discrepancies were found at all, skip writing the file
    if not discrepancies:
        logger.info("No discrepancies found between portal_issn and current collections. No CSV created.")
        return

    # Output to CSV file
    file_export_path = Variable.get("OAI_DC_INPUT_PATH")
    issn_export_path = os.path.join(file_export_path, "issn")

    os.makedirs(issn_export_path, exist_ok=True)

    logger.info("Diretório de exportação: %s", issn_export_path)

    csv_filename = os.path.join(
        issn_export_path, f"issn_discrepancies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

    fieldnames = ['issn']
    for item in discrepancies:
        for key in item.keys():
            if key != 'issn' and key not in fieldnames:
                fieldnames.append(key)

    with open(csv_filename, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in discrepancies:
            writer.writerow(row)
            
    logger.info("CSV successfully written to %s with %d discrepancies.", csv_filename, len(discrepancies))


default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    'ISSN_01_diff_current_portal',
    default_args=default_args,
    description=('Compara portal_issn e current no MongoDB'),
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['mongodb', 'portal_issn', 'current', 'diff'],
) as dag:
    compare_task = PythonOperator(
        task_id='compare_portal_issn_with_current',
        python_callable=compare_issn_records,
        provide_context=True,
    )