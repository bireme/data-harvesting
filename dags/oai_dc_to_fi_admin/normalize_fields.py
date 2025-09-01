import re
from airflow.providers.mysql.hooks.mysql import MySqlHook

def get_source_sas_id(db_conn, journal, year, volume, number):
  sql_volume = ""
  if (volume):
    sql_volume = " a.volume_serial = '%s' AND " % volume

  sql_number = ""
  if (number):
    sql_number = " a.issue_number = '%s' AND " % number

  sql = """
    SELECT
      b.id as code
    FROM
      biblioref_referencesource AS a,
      biblioref_reference AS b
    WHERE
      a.title_serial = %s AND
      a.reference_ptr_id = b.id AND """ + sql_volume + sql_number + """
      LEFT(b.publication_date_normalized, 4) = %s
    ORDER BY
      CASE
        WHEN b.status = -1 THEN -4
        ELSE b.status
      END DESC
    LIMIT 1
  """

  id = None
  with db_conn.cursor() as cursor:
    cursor.execute(sql, (journal, year))
    result = cursor.fetchone()
    if result:
      id = result[0]

  return id


def get_short_title_and_issn(db_conn, journal, issn):
  sql = """
    SELECT
      coalesce(NULLIF(a.shortened_title, ''), a.title),
      a.issn
    FROM
      title_title AS a
    WHERE
      (a.shortened_title = %s OR a.title = %s OR a.issn = %s)
    LIMIT 1
  """

  journal_issn = {}
  with db_conn.cursor() as cursor:
    cursor.execute(sql, (journal, journal, issn))
    result = cursor.fetchone()
    if result:
      journal_issn['journal'] = result[0]
      journal_issn['issn'] = result[1]

  return journal_issn


def is_date_valid(date_string):
    pattern = re.compile(r'^\d{8}$')
    return bool(pattern.match(date_string))


def normalize_lang_code(code):
    lang_code = code.strip().lower()

    if lang_code in ["por", "pt-br"]:
        lang_code = "pt"
    elif lang_code in ["eng", "en-us"]:
        lang_code = "en"
    elif lang_code in ["spa", "es-es"]:
        lang_code = "es"
    elif lang_code in ["fre", "fr-fr"]:
        lang_code = "fr"

    return lang_code

def normalize_pagination(pages):
    pages_normalized = []

    if '-' in pages:
        pages = pages.split('-')
        pages_f = int(pages[0])
        pages_l = int(pages[1])

        if pages_l < pages_f:
            pages_l = pages_f + pages_l

        pages_json = {'_f': str(pages_f), '_l': str(pages_l)}
        pages_normalized.append(pages_json)
    else:
       pages_json = {'_f': pages, '_l': pages}

    return pages_normalized
