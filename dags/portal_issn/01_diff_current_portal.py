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


def extract_control_field(fields_array, field_code):
    for field_obj in fields_array:
        if field_code in field_obj:
            val = field_obj[field_code]
            if isinstance(val, str):
                return val
            elif isinstance(val, dict):
                return val.get('value') or val.get('_') or str(val)
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
            portal_title = extract_field_value(fields, '222', 'a')
            portal_subtitle = extract_field_value(fields, '222', 'b')

            current_title = current_record.get('title')
            current_subtitle = current_record.get('subtitle')
            current_id_number = current_record.get('id_number')

            diffs = {}

            if portal_title and portal_title != current_title:
                diffs['title'] = portal_title

            if portal_subtitle and portal_subtitle != current_subtitle:
                diffs['subtitle'] = portal_subtitle

            if diffs:
                row_data = {
                    'issn': issn,
                    'id_number': current_id_number
                }
                row_data.update(diffs)
                discrepancies.append(row_data)

    if not discrepancies:
        logger.info("No title/subtitle discrepancies found between portal_issn and current collections. No CSV created.")
        return

    file_export_path = Variable.get("OAI_DC_INPUT_PATH")
    issn_export_path = os.path.join(file_export_path, "issn")

    os.makedirs(issn_export_path, exist_ok=True)

    csv_filename = os.path.join(
        issn_export_path, f"issn_discrepancies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

    fieldnames = ['issn', 'id_number']
    for item in discrepancies:
        for key in item.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    with open(csv_filename, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in discrepancies:
            writer.writerow(row)
            
    logger.info("CSV successfully written to %s with %d discrepancies.", csv_filename, len(discrepancies))


def compare_dates_and_status_records(**context):
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

        issn = extract_field_value(fields, '022', 'a')
        if not issn:
            continue

        current_record = current_coll.find_one({'issn': issn})
        if current_record:
            id_number = current_record.get('id_number')

            # Extract field 008
            field_008 = extract_control_field(fields, '008')
            portal_initial_date = None
            portal_final_date = None

            if field_008 and len(field_008) >= 15:
                # Chars 07 to 10 (Python index 7 to 11)
                portal_initial_date = field_008[7:11]
                # Chars 11 to 14 (Python index 11 to 15)
                portal_final_date = field_008[11:15]
            print(f"ISSN: {issn}, Portal Initial Date: {portal_initial_date}, Portal Final Date: {portal_final_date}")

            current_status = current_record.get('status')
            current_initial_date = current_record.get('initial_date')
            current_final_date = current_record.get('final_date')

            if (
                (portal_initial_date and portal_initial_date != current_initial_date) or
                (portal_final_date and portal_final_date != current_final_date)
            ):
                row_data = {
                    'id_number': id_number,
                    'issn': issn,
                    'status': current_status,
                    'initial_date': portal_initial_date if portal_initial_date else current_initial_date,
                    'final_date': portal_final_date if portal_final_date else current_final_date,
                }
                discrepancies.append(row_data)

    if not discrepancies:
        logger.info("No date/status discrepancies found. No CSV created.")
        return

    file_export_path = Variable.get("OAI_DC_INPUT_PATH")
    issn_export_path = os.path.join(file_export_path, "issn")

    os.makedirs(issn_export_path, exist_ok=True)

    csv_filename = os.path.join(
        issn_export_path, f"issn_dates_status_discrepancies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

    fieldnames = ['id_number', 'issn', 'status', 'initial_date', 'final_date']

    with open(csv_filename, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in discrepancies:
            writer.writerow(row)

    logger.info("Dates/status CSV successfully written to %s with %d records.", csv_filename, len(discrepancies))


default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    'ISSN_01_diff_current_portal',
    default_args=default_args,
    description=('Compara portal_issn e current no MongoDB (Títulos e Datas/Status)'),
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

    compare_dates_status_task = PythonOperator(
        task_id='compare_dates_and_status_task',
        python_callable=compare_dates_and_status_records,
        provide_context=True,
    )