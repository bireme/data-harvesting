from datetime import datetime, timedelta
import csv
import logging
import os
import re
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


def parse_marc_362(text):
    """
    Parses MARC field 362 subfield $a to extract start and optional final volume, number, and year.
    Example 1: Vol. 35, no. 1 (Jan. 1969)-
    Example 2: Vol. 21, no. 3 (Mar. 1979)-v. 41, no. 6 (Nov./Dec. 1999).
    """
    if not text:
        return None, None, None, None, None, None
    
    text = text.strip()
    pattern = re.compile(
        r'(?:[Vv]ol\.?|v\.?)\s*([^,]+),\s*no\.?\s*([^\(]+)\s*\([^\)]*?(\d{4})\)'
        r'(?:.*?-\s*(?:[Vv]ol\.?|v\.?)\s*([^,]+),\s*no\.?\s*([^\(]+)\s*\([^\)]*?(\d{4})\))?',
        re.IGNORECASE
    )
    
    match = pattern.search(text)
    if match:
        start_vol = match.group(1).strip()
        start_no = match.group(2).strip()
        start_year = match.group(3).strip()
        
        end_vol = match.group(4).strip() if match.group(4) else None
        end_no = match.group(5).strip() if match.group(5) else None
        end_year = match.group(6).strip() if match.group(6) else None
        
        return start_vol, start_no, start_year, end_vol, end_no, end_year
    
    return None, None, None, None, None, None


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

            # 1. Extract Field 008 (Status and Dates)
            field_008 = extract_control_field(fields, '008')
            portal_status = None
            portal_initial_date = None
            portal_final_date = None

            if field_008:
                if len(field_008) > 6:
                    portal_status = field_008[6]
                
                if len(field_008) >= 15:
                    portal_initial_date = field_008[7:11]
                    portal_final_date = field_008[11:15]

            # 2. Extract Field 362 (Volumes, Numbers, Years)
            field_362 = extract_field_value(fields, '362', 'a')
            start_vol, start_no, start_year, final_vol, final_no, final_year = parse_marc_362(field_362)

            # Get current record values
            current_status = current_record.get('status')
            current_initial_date = current_record.get('initial_date')
            current_final_date = current_record.get('final_date')
            current_start_vol = current_record.get('start_volume')
            current_start_no = current_record.get('start_number')
            current_start_year = current_record.get('start_year')
            current_final_vol = current_record.get('final_volume')
            current_final_no = current_record.get('final_number')
            current_final_year = current_record.get('final_year')

            # Check for discrepancies across 008 and 362 fields
            if (
                (portal_status and current_status and portal_status.lower() != current_status.lower()) or
                (portal_initial_date and portal_initial_date != current_initial_date) or
                (portal_final_date and portal_final_date != current_final_date) or
                (start_vol and start_vol != current_start_vol) or
                (start_no and start_no != current_start_no) or
                (start_year and start_year != current_start_year) or
                (final_vol and final_vol != current_final_vol) or
                (final_no and final_no != current_final_no) or
                (final_year and final_year != current_final_year)
            ):
                row_data = {
                    'id_number': id_number,
                    'issn': issn,
                    'status': portal_status,
                    'initial_date': portal_initial_date,
                    'final_date': portal_final_date,
                    'start_volume': start_vol,
                    'start_number': start_no,
                    'start_year': start_year,
                    'final_volume': final_vol,
                    'final_number': final_no,
                    'final_year': final_year,
                }
                discrepancies.append(row_data)

    if not discrepancies:
        logger.info("No date, status, or MARC 362 discrepancies found. No CSV created.")
        return

    file_export_path = Variable.get("OAI_DC_INPUT_PATH")
    issn_export_path = os.path.join(file_export_path, "issn")

    os.makedirs(issn_export_path, exist_ok=True)

    csv_filename = os.path.join(
        issn_export_path, f"issn_dates_status_discrepancies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

    fieldnames = [
        'id_number', 'issn', 'status', 'initial_date', 'final_date',
        'start_volume', 'start_number', 'start_year',
        'final_volume', 'final_number', 'final_year'
    ]

    with open(csv_filename, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in discrepancies:
            writer.writerow(row)

    logger.info("Consolidated CSV successfully written to %s with %d records.", csv_filename, len(discrepancies))


default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    'ISSN_01_diff_current_portal',
    default_args=default_args,
    description=('Compara portal_issn e current no MongoDB (Títulos, Datas, Status e 362)'),
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