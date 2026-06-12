import requests
import re
import logging
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.exceptions import AirflowException


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


def is_doi(s):
    doi_pattern = re.compile(r'^10\.\d{4,}/\S+$')
    return bool(doi_pattern.match(s))


def is_article_url(s):
    ojs_pattern = re.compile(r'/(?:[^/]+/)+article/view/\d+(/[^/]+)?$', re.IGNORECASE)
    return bool(ojs_pattern.search(s))


def is_issn(s):
    issn_pattern = re.compile(r'^\d{4}-\d{4}$')
    return bool(issn_pattern.match(s))


def get_journal_data(journal_id):
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    collection = mongo_hook.get_collection('current', 'TITLE')
    query = {"id": int(journal_id)}
    projection = {"issn": 1, "shortened_title": 1, "title": 1, "_id": 0}
    result = collection.find_one(query, projection)

    journal_data = {}
    if result:
        journal_data['journal'] = result.get('shortened_title') or result.get('title')
        journal_data['issn'] = result.get('issn')

    return journal_data


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